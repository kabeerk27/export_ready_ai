"""
Offline Data Ingester: PDF -> Regex -> SQLite
Extracts HS codes, export conditions, and dates from DGFT notification PDFs.
NO API NEEDED - uses pdfplumber + regex pattern matching.

For scanned PDFs: install Tesseract OCR and this script will auto-detect and use it.

Usage:
  python offline_ingester.py                    # Process all PDFs
  python offline_ingester.py --limit 20         # Process first 20 new PDFs
  python offline_ingester.py --file downloads/some_file.pdf  # Single file
"""

import os
import sys
import json
import re
import sqlite3
import argparse
import warnings

import pdfplumber  # type: ignore

# Suppress pdfplumber font warnings
warnings.filterwarnings("ignore", message=".*FontBBox.*")

# ---- Try importing OCR tools (optional) ----
HAS_OCR = False
POPPLER_PATH = None
try:
    from pdf2image import convert_from_path  # type: ignore
    import pytesseract  # type: ignore
    local_poppler = os.path.join(os.path.dirname(__file__), "poppler-24.08.0", "Library", "bin")
    if os.path.isdir(local_poppler):
        POPPLER_PATH = local_poppler

    # Quick check if tesseract is available
    pytesseract.get_tesseract_version()
    HAS_OCR = True
except Exception:
    HAS_OCR = False

# ---- CONFIG ----
DOWNLOADS_DIR = "downloads"
DATABASE = "export_brain.db"
INGESTION_LOG = "offline_ingested.json"

# ---- REGEX PATTERNS ----

# Dotted HS codes (high confidence): 0101.21, 6101.10.00, etc.
HS_DOTTED_PATTERN = re.compile(r'\b(\d{4}\.\d{2}(?:\.\d{2})?)\b')

# Plain 4-digit HS codes (need context validation)
HS_PLAIN_PATTERN = re.compile(r'\b(\d{4})\b')

# Keywords that indicate an HS code is nearby
HS_CONTEXT_KEYWORDS = re.compile(
    r'(?:hs\s*code|itc|tariff|heading|sub.?heading|chapter|customs|schedule|'
    r'export\s*policy|import\s*policy|ctsh|cth|serial)',
    re.IGNORECASE
)

# Things that look like HS codes but aren't
FALSE_POSITIVE_CONTEXT = re.compile(
    r'(?:tel|phone|fax|pin|ext|no\.|number|page|dated|circular|notification\s*no|'
    r'para\s|clause|section|order\s*no|file\s*no|sr\.?\s*no)',
    re.IGNORECASE
)

# Date patterns
DATE_PATTERNS = [
    re.compile(r'\b(\d{1,2})[./-](\d{1,2})[./-](20\d{2})\b'),
    re.compile(r'\b(\d{1,2})\s*(?:st|nd|rd|th)?\s*(January|February|March|April|May|June|July|August|September|October|November|December)\s*,?\s*(20\d{2})\b', re.IGNORECASE),
    re.compile(r'\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})\s*(?:st|nd|rd|th)?\s*,?\s*(20\d{2})\b', re.IGNORECASE),
]

MONTH_MAP = {
    'january': '01', 'february': '02', 'march': '03', 'april': '04',
    'may': '05', 'june': '06', 'july': '07', 'august': '08',
    'september': '09', 'october': '10', 'november': '11', 'december': '12',
}

CONDITION_KEYWORDS = [
    'free', 'prohibited', 'restricted', 'banned', 'suspended',
    'subject to', 'license', 'licence', 'permit', 'quota',
    'condition', 'allowed', 'not allowed', 'MEP', 'minimum export price',
    'STE', 'state trading', 'SCOMET', 'export duty', 'cess',
    'QCO', 'quality control order',
]


# ---- HELPERS ----

def load_ingestion_log() -> set:
    if os.path.exists(INGESTION_LOG):
        with open(INGESTION_LOG, "r") as f:
            return set(json.load(f))
    return set()


def save_ingestion_log(log: set):
    with open(INGESTION_LOG, "w") as f:
        json.dump(list(log), f, indent=2)


def extract_text_pdfplumber(pdf_path: str) -> str:
    text_pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_pages.append(page_text)
    return "\n".join(text_pages)


def extract_text_ocr(pdf_path: str) -> str:
    """Use pdf2image + pytesseract for scanned PDFs."""
    if not HAS_OCR:
        return ""
    try:
        if POPPLER_PATH:
            images = convert_from_path(
                pdf_path, poppler_path=POPPLER_PATH,
                dpi=200, first_page=1, last_page=10,
            )
        else:
            images = convert_from_path(
                pdf_path, dpi=200, first_page=1, last_page=10,
            )
        text_pages = []
        for img in images:
            text = pytesseract.image_to_string(img, lang='eng')
            if text.strip():
                text_pages.append(text)
        return "\n".join(text_pages)
    except Exception as e:
        print(f"    [!] OCR error: {e}")
        return ""


def extract_dates(text: str) -> list:
    dates = []
    for match in DATE_PATTERNS[0].finditer(text):
        day, month, year = match.groups()
        day, month = day.zfill(2), month.zfill(2)
        if 1 <= int(month) <= 12 and 1 <= int(day) <= 31:
            dates.append(f"{year}-{month}-{day}")
    for match in DATE_PATTERNS[1].finditer(text):
        day, month_name, year = match.groups()
        month = MONTH_MAP.get(month_name.lower(), '00')
        dates.append(f"{year}-{month}-{day.zfill(2)}")
    for match in DATE_PATTERNS[2].finditer(text):
        month_name, day, year = match.groups()
        month = MONTH_MAP.get(month_name.lower(), '00')
        dates.append(f"{year}-{month}-{day.zfill(2)}")
    return list(set(dates))


def get_context(text: str, pos: int, window: int = 150) -> str:
    """Get text surrounding a position."""
    start = max(0, pos - window)
    end = min(len(text), pos + window)
    return text[start:end]  # type: ignore[index]


def find_condition_near_hs(text: str, hs_pos: int) -> str:
    context = get_context(text, hs_pos, 200).lower()
    found = [kw.title() for kw in CONDITION_KEYWORDS if kw in context]
    if found:
        return "; ".join(found[:3])  # type: ignore
    # Fallback: grab the line
    line_start = text.rfind('\n', 0, hs_pos) + 1
    line_end = text.find('\n', hs_pos)
    if line_end == -1:
        line_end = min(hs_pos + 150, len(text))
    line = text[line_start:line_end].strip()  # type: ignore[index]
    # Clean non-ASCII for safe printing
    line = line.encode('ascii', 'replace').decode('ascii')
    return line[:150]


def is_valid_hs_code(code: str, text: str, pos: int) -> bool:
    """Validate if a matched number is likely a real HS code."""
    chapter = int(code[:2])  # type: ignore
    if chapter < 1 or chapter > 97:
        return False
    code_num = int(code.replace('.', '')[:4])  # type: ignore
    # Filter years
    if 1900 <= code_num <= 2030:
        return False
    # Filter very common non-HS numbers
    if code in ('1000', '2000', '3000', '5000', '1001', '1100'):
        return False
    # Check context for false positive indicators
    context = get_context(text, pos, 80)
    if FALSE_POSITIVE_CONTEXT.search(context):
        return False
    return True


def extract_records(text: str) -> list:
    """Extract HS codes, conditions, and dates from text using regex."""
    records = []
    seen_codes = set()

    dates = extract_dates(text[:2000])  # type: ignore
    effective_date = dates[0] if dates else ""

    # Check if this document has trade/HS context at all
    has_trade_context = bool(HS_CONTEXT_KEYWORDS.search(text[:5000]))  # type: ignore

    # 1. High-confidence: Dotted HS codes (e.g., 0101.21, 6101.10.00)
    for match in HS_DOTTED_PATTERN.finditer(text):
        hs_code = match.group(1)
        if hs_code in seen_codes:
            continue
        if not is_valid_hs_code(hs_code, text, match.start()):
            continue
        seen_codes.add(hs_code)
        condition = find_condition_near_hs(text, match.start())
        records.append({"hs_code": hs_code, "export_condition": condition, "effective_date": effective_date})

    # 2. Plain 4-digit codes: only if document has trade context
    if has_trade_context:
        for match in HS_PLAIN_PATTERN.finditer(text):
            hs_code = match.group(1)
            if hs_code in seen_codes:
                continue
            if not is_valid_hs_code(hs_code, text, match.start()):
                continue
            # For plain 4-digit, require HS/trade keyword nearby
            context = get_context(text, match.start(), 120)
            if not HS_CONTEXT_KEYWORDS.search(context):
                continue
            seen_codes.add(hs_code)
            condition = find_condition_near_hs(text, match.start())
            records.append({"hs_code": hs_code, "export_condition": condition, "effective_date": effective_date})

    return records


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
    parser = argparse.ArgumentParser(description="Offline DGFT PDF Ingester (no API needed)")
    parser.add_argument("--file", type=str, help="Process a single PDF file")
    parser.add_argument("--limit", type=int, default=0, help="Max PDFs to process (0 = all)")
    args = parser.parse_args()

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
    skipped_scan = 0

    print("=" * 60)
    print("OFFLINE DGFT PDF INGESTER (No API Required)")
    print("=" * 60)
    print(f"Found {len(pdf_files)} PDF(s). Already ingested: {len(ingested)}.")
    print(f"OCR available: {'YES (Tesseract + Poppler)' if HAS_OCR else 'NO (text-based PDFs only)'}")
    print("-" * 60)

    for pdf_path in pdf_files:
        filename = os.path.basename(pdf_path)
        if filename in ingested:
            continue
        if args.limit and processed >= args.limit:
            print(f"\nReached limit of {args.limit} files.")
            break

        processed += 1  # type: ignore

        # Step 1: Try text extraction
        text = extract_text_pdfplumber(pdf_path)

        if not text.strip():
            # Try OCR if available
            if HAS_OCR:
                print(f"[{processed}] {filename[:55]}...")  # type: ignore
                print(f"    [OCR] Running Tesseract OCR...")
                text = extract_text_ocr(pdf_path)
                if not text.strip():
                    print(f"    [SKIP] OCR could not extract text.")
                    ingested.add(filename)
                    skipped_scan += 1  # type: ignore
                    continue
            else:
                skipped_scan += 1  # type: ignore
                ingested.add(filename)
                continue

        # Step 2: Extract records using regex
        records = extract_records(text)

        if records:
            count = insert_into_db(records, filename)
            total_records += count  # type: ignore
            print(f"[{processed}] {filename[:55]}...")  # type: ignore
            print(f"    Found {len(records)} HS code(s) -> Inserted {count} record(s)")
        # If no records found, just silently skip (many notifications are administrative)

        ingested.add(filename)

    save_ingestion_log(ingested)
    print("\n" + "=" * 60)
    print(f"DONE!")
    print(f"  Processed: {processed} PDF(s)")
    print(f"  Scanned/skipped: {skipped_scan}")
    print(f"  Records inserted: {total_records}")
    print("=" * 60)


if __name__ == "__main__":
    main()
