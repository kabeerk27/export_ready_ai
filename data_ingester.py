"""
Data Ingester: PDF -> Gemini LLM -> SQLite
Reads DGFT notification PDFs, extracts structured data via Gemini, and stores it in export_brain.db.

Pipeline:
  1. Try pdfplumber text extraction
  2. If no text (scanned PDF): convert pages to JPG via pdf2image + Poppler
  3. Send text or images to Gemini for structured extraction
  4. Insert results into SQLite

Usage:
  python data_ingester.py --api-key YOUR_KEY --file downloads/some_file.pdf
  python data_ingester.py --api-key YOUR_KEY --limit 5
  python data_ingester.py --api-key YOUR_KEY
"""

import os
import sys
import json
import re
import sqlite3
import argparse
import time
import io
import base64

import pdfplumber  # type: ignore
from pdf2image import convert_from_path  # type: ignore
from PIL import Image  # type: ignore
from google import genai  # type: ignore
from google.genai import types  # type: ignore

# ---- CONFIG ----
DOWNLOADS_DIR = "downloads"
DATABASE = "export_brain.db"
INGESTION_LOG = "ingested_files.json"
POPPLER_PATH = os.path.join(os.path.dirname(__file__), "poppler-24.08.0", "Library", "bin")
DELAY_BETWEEN_FILES = 8
MAX_RETRIES = 3

TEXT_PROMPT = """I am an export compliance startup. From the following DGFT notification text, extract ALL relevant entries. For each entry, extract:
1. HS Code (the tariff/product code, e.g., "0101", "6101.10")
2. Export Condition (the rule, restriction, or condition applied)
3. Effective Date (in YYYY-MM-DD format if possible)

Return ONLY a valid JSON array with keys: "hs_code", "export_condition", "effective_date".
If no HS codes are found, return an empty array: []

--- START OF DGFT TEXT ---
"""

VISION_PROMPT = """You are an expert trade lawyer. Transcribe all the text from this image and identify:
1. HS Code (tariff/product code)
2. Export Policy / Export Condition
3. Effective Date

Return ONLY a valid JSON array with keys: "hs_code", "export_condition", "effective_date".
If no HS codes are found, return an empty array: []
"""


# ---- HELPERS ----

def load_ingestion_log() -> set:
    if os.path.exists(INGESTION_LOG):
        with open(INGESTION_LOG, "r") as f:
            return set(json.load(f))
    return set()


def save_ingestion_log(log: set):
    with open(INGESTION_LOG, "w") as f:
        json.dump(list(log), f, indent=2)


def extract_text_from_pdf(pdf_path: str) -> str:
    text_pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_pages.append(page_text)
    return "\n".join(text_pages)


def convert_pdf_to_images(pdf_path: str, max_pages: int = 5) -> list:
    """Convert PDF pages to JPG images using pdf2image + Poppler."""
    try:
        images = convert_from_path(
            pdf_path,
            poppler_path=POPPLER_PATH,
            dpi=200,
            first_page=1,
            last_page=max_pages,
            fmt="jpeg",
        )
        return images
    except Exception as e:
        print(f"    [!] pdf2image conversion error: {e}")
        return []


def image_to_bytes(img) -> bytes:
    """Convert PIL Image to JPEG bytes."""
    buffer = io.BytesIO()
    img.save(buffer, format="JPEG", quality=85)
    return buffer.getvalue()


def parse_gemini_json(raw: str) -> list:
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1]
    if raw.endswith("```"):
        raw = raw.rsplit("```", 1)[0]
    raw = raw.strip()
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return data
    except json.JSONDecodeError:
        pass
    return []


def get_retry_delay(error_msg: str) -> int:
    match = re.search(r'retryDelay.*?(\d+)', str(error_msg))
    if match:
        return int(match.group(1)) + 5
    return 60


def call_with_retry(func, max_retries=MAX_RETRIES):
    """Call a function with automatic retry on rate limit (429) errors."""
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            error_str = str(e)
            if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                wait_time = get_retry_delay(error_str)
                if attempt < max_retries - 1:
                    print(f"    [WAIT] Rate limited. Waiting {wait_time}s before retry ({attempt+1}/{max_retries})...")
                    time.sleep(wait_time)
                else:
                    print(f"    [!] Rate limit persists after {max_retries} retries. Skipping.")
                    return None
            else:
                print(f"    [!] Gemini API error: {e}")
                return None
    return None


def ask_gemini_text(text: str, client) -> list:
    """Send extracted text to Gemini."""
    max_chars = 30000
    truncated = text
    if len(text) > max_chars:
        truncated = text[:max_chars] + "\n\n[...TEXT TRUNCATED...]"  # type: ignore[index]
    full_prompt = TEXT_PROMPT + truncated + "\n--- END OF DGFT TEXT ---"

    def _call():
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=full_prompt,
        )
        return parse_gemini_json(response.text if response.text else "")

    result = call_with_retry(_call)
    return result if result is not None else []


def ask_gemini_vision(images: list, client) -> list:
    """Send JPG images to Gemini Vision for OCR + extraction."""
    # Build parts: all images + the prompt
    parts = []
    for i, img in enumerate(images):
        jpg_bytes = image_to_bytes(img)
        parts.append(types.Part.from_bytes(data=jpg_bytes, mime_type="image/jpeg"))
        print(f"    [IMG] Page {i+1} converted to JPG ({len(jpg_bytes)//1024}KB)")

    parts.append(types.Part.from_text(text=VISION_PROMPT))

    def _call():
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=[types.Content(parts=parts)],
        )
        return parse_gemini_json(response.text if response.text else "")

    result = call_with_retry(_call)
    return result if result is not None else []


def insert_into_db(records: list, source_file: str):
    if not records:
        return 0
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    count = 0
    for r in records:
        hs_code = r.get("hs_code", "").strip()
        condition = r.get("export_condition", "").strip()
        eff_date = r.get("effective_date", "").strip()
        if not hs_code:
            continue
        c.execute(
            "INSERT INTO RegulationMaster (hs_code, country, rule_type, details, date_updated) VALUES (?, ?, ?, ?, ?)",
            (hs_code, "India-DGFT", "Export Condition", condition, eff_date),
        )
        count += 1
    conn.commit()
    conn.close()
    return count


# ---- MAIN ----

def main():
    parser = argparse.ArgumentParser(description="Ingest DGFT PDFs into export_brain.db via Gemini")
    parser.add_argument("--file", type=str, help="Process a single PDF file")
    parser.add_argument("--limit", type=int, default=0, help="Max PDFs to process (0 = all)")
    parser.add_argument("--api-key", type=str, help="Gemini API key")
    args = parser.parse_args()

    api_key = args.api_key or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("ERROR: Provide your Gemini API key!")
        print("  python data_ingester.py --api-key YOUR_KEY")
        sys.exit(1)

    # Verify Poppler is available
    if not os.path.isdir(POPPLER_PATH):
        print(f"WARNING: Poppler not found at {POPPLER_PATH}")
        print("Scanned PDFs will be skipped. Download Poppler to fix this.")

    client = genai.Client(api_key=api_key)

    if args.file:
        if not os.path.exists(args.file):
            print(f"ERROR: File not found: {args.file}")
            sys.exit(1)
        pdf_files = [args.file]
    else:
        if not os.path.isdir(DOWNLOADS_DIR):
            print(f"ERROR: Downloads folder '{DOWNLOADS_DIR}' not found.")
            sys.exit(1)
        pdf_files = [
            os.path.join(DOWNLOADS_DIR, f)
            for f in sorted(os.listdir(DOWNLOADS_DIR))
            if f.lower().endswith(".pdf")
        ]

    ingested = load_ingestion_log()
    processed = 0
    total_records = 0

    print(f"Found {len(pdf_files)} PDF(s). Already ingested: {len(ingested)}.")
    print(f"Poppler: {POPPLER_PATH}")
    print("=" * 60)

    for pdf_path in pdf_files:
        filename = os.path.basename(pdf_path)
        if filename in ingested:
            continue
        if args.limit and processed >= args.limit:
            print(f"\nReached limit of {args.limit} files. Stopping.")
            break

        processed += 1
        print(f"\n[{processed}] {filename}")

        # Step 1: Try text extraction
        text = extract_text_from_pdf(pdf_path)

        if text.strip():
            # Text-based PDF
            print(f"    [TEXT] {len(text)} chars extracted. Sending to Gemini...")
            records = ask_gemini_text(text, client)
        else:
            # Scanned PDF -> convert to JPG images
            print("    [SCAN] No text found. Converting pages to JPG...")
            images = convert_pdf_to_images(pdf_path)
            if images:
                print(f"    [VISION] Sending {len(images)} page image(s) to Gemini Vision...")
                records = ask_gemini_vision(images, client)
            else:
                print("    [!] Could not convert PDF to images. Skipping.")
                records = []

        print(f"    [RESULT] {len(records)} record(s) found.")

        if records:
            count = insert_into_db(records, filename)
            total_records += count
            print(f"    [DB] Inserted {count} record(s).")
        else:
            print("    [--] No HS code data in this notification.")

        ingested.add(filename)
        save_ingestion_log(ingested)
        time.sleep(DELAY_BETWEEN_FILES)

    print("\n" + "=" * 60)
    print(f"Done! Processed {processed} PDF(s). Inserted {total_records} total record(s).")


if __name__ == "__main__":
    main()
