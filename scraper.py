import os
import requests # type: ignore
from bs4 import BeautifulSoup # type: ignore
import json
import re
from typing import Set

DOWNLOAD_DIR = 'downloads'
HISTORY_FILE = 'download_history.json'

def load_history() -> Set[str]:
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r') as f:
            return set(json.load(f))
    return set()

def save_history(history):
    with open(HISTORY_FILE, 'w') as f:
        json.dump(list(history), f)

def main():
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)

    history: Set[str] = load_history()
    
    session = requests.Session()
    # Mask as a normal browser
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })

    main_url = 'https://www.dgft.gov.in/CP/?opt=notification'
    
    print(f"Fetching initial page: {main_url}")
    try:
        response = session.get(main_url, timeout=30)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to load main page: {e}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    csrf_token_meta = soup.find('meta', {'name': '_csrf'})
    
    if not csrf_token_meta:
        print("Could not find CSRF token on the main page. Exiting.")
        return
        
    csrf_token = csrf_token_meta.get('content')
    print(f"Acquired CSRF token: {csrf_token}")

    search_api_url = f"https://www.dgft.gov.in/CP/webHP?requestType=ApplicationRH&actionVal=getNewsDtlsBySearch&isPrivate=true&screenId=90000734&_csrf={csrf_token}"
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'X-CSRF-TOKEN': csrf_token,
        'Referer': main_url,
    }
    
    data = 'number=&catName=All&assetCat=0&subject=&year=&fromDate=&toDate=&assetCatName=Notification'

    print("Fetching notification data from search endpoint...")
    try:
        search_res = session.post(search_api_url, data=data, headers=headers, timeout=30)
        search_res.raise_for_status()
    except Exception as e:
        print(f"Failed to fetch data from search API: {e}")
        return

    # Parse the table data
    table_soup = BeautifulSoup(search_res.text, 'html.parser')
    rows = table_soup.find_all('tr')
    
    print(f"Found {len(rows)} rows to process.")
    
    downloaded_count = 0
    for row in rows:
        attachment_links = row.find_all('a', class_='attachmentBtn')
        if not attachment_links:
            continue
            
        cells = row.find_all('td')
        if not cells or len(cells) < 4:
            continue
            
        # Get notification number, let's use it as filename prefix (sanitizing it)
        notif_num = cells[1].text.strip()
        sanitized_num = re.sub(r'[^a-zA-Z0-9_\-]', '_', notif_num)
        
        for link in attachment_links:
            href = link.get('href')
            if not href:
                # In some portals, the JS function handles it like javascript:openAttachment(...)
                onclick = link.get('onclick')
                if onclick:
                    # Very simple regex to grab the URL if it's passed into javascript
                    match = re.search(r"'(.*?\.pdf)'", onclick) or re.search(r'"(.*?\.pdf)"', onclick)
                    if match:
                        href = match.group(1)

            if not href:
                continue
                
            if href.startswith('http'):
                pdf_url = href
            else:
                pdf_url = f"https://www.dgft.gov.in{href if href.startswith('/') else '/' + href}"

            if pdf_url in history:
                print(f"Skipping already downloaded file: {pdf_url}")
                continue

            # PDF name logic: use sanitized_num combined with the last part of pdf_url
            pdf_filename = f"{sanitized_num}_{pdf_url.split('/')[-1]}"
            # Sometimes URL contains query parameters, so we clean it up
            pdf_filename = pdf_filename.split('?')[0]
            if not pdf_filename.endswith('.pdf'):
                pdf_filename += '.pdf'
                
            pdf_path = os.path.join(DOWNLOAD_DIR, pdf_filename)
            
            print(f"Downloading {pdf_url} to {pdf_path}")
            try:
                pdf_res = session.get(pdf_url, stream=True, timeout=30)
                pdf_res.raise_for_status()
                with open(pdf_path, 'wb') as f:
                    for chunk in pdf_res.iter_content(chunk_size=8192):
                        f.write(chunk)
                history.add(pdf_url)
                downloaded_count += 1
            except Exception as e:
                print(f"Failed to download {pdf_url}: {e}")

    save_history(history)
    print(f"Finished. Downloaded {downloaded_count} new notifications.")

if __name__ == "__main__":
    main()
