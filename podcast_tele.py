import argparse
import hashlib
import os
import re
from datetime import datetime
from urllib.parse import urljoin

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeoutError

def sanitize_filename(name: str) -> str:
    """Eltávolítja a fájlrendszer számára érvénytelen karaktereket."""
    return re.sub(r'[\\/*?:"<>|]', "", name).strip()

def download_file_with_requests(url: str, filepath: str):
    """Közvetlenül letölti a fájlt az URL-ről."""
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(filepath, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

def main():
    parser = argparse.ArgumentParser(description="Podcast tömeges letöltő script")
    parser.add_argument('--startpage', type=int, default=1, help='Kezdő oldal száma')
    parser.add_argument('--endpage', type=int, default=100, help='Utolsó oldal száma')
    parser.add_argument('--out', type=str, default='./podcasts', help='Kimeneti mappa a letöltéseknek')
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    visited_file = "visited_podcast.txt"
    
    visited = set()
    if os.path.exists(visited_file):
        with open(visited_file, 'r', encoding='utf-8') as f:
            visited = set(line.strip() for line in f)

    current_year = datetime.now().year

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        for page_num in range(args.startpage, args.endpage + 1):
            url = f"https://podcast.hu/kereses?search=&type=episode&category=&duration=&page={page_num}"
            print(f"\n--- Oldal betöltése: {page_num}. oldal ---")
            page.goto(url)
            
            # Sütik elfogadása
            try:
                page.wait_for_selector("text=Mindegyik elfogadása", timeout=3000)
                page.locator("text=Mindegyik elfogadása").click()
                print("✅ Süti banner leokézva.")
            except PwTimeoutError:
                pass
            
            try:
                page.wait_for_selector("text=Olvass tovább", timeout=10000)
            except PwTimeoutError:
                print(f"Nincsenek találatok a(z) {page_num}. oldalon, vagy a végére értünk.")
                break

            count = page.locator("text=Olvass tovább").count()
            if count == 0:
                break

            for i in range(count):
                try:
                    page.wait_for_selector("text=Olvass tovább", timeout=5000)
                    read_more_btns = page.locator("text=Olvass tovább")
                    
                    if i >= read_more_btns.count():
                        break
                    
                    read_more_btns.nth(i).click()
                    
                    # --- JAVÍTÁS 1: Csak a LÁTHATÓ h1-et várjuk meg ---
                    page.wait_for_selector("h1:visible", timeout=10000)
                    # Az audio tag lehet, hogy rejtett, ezért a state="attached"-et használjuk
                    page.wait_for_selector("audio", state="attached", timeout=10000)
                    
                    # Csak a látható címet kérjük le
                    title = page.locator("h1:visible").first.inner_text().strip()
                    title_hash = hashlib.md5(title.encode('utf-8')).hexdigest()[:8]
                    
                    if title_hash in visited:
                        print(f"Már letöltve, ugrás: {title}")
                        page.go_back()
                        continue

                    body_text = page.locator("body").inner_text()
                    
                    date_match = re.search(r'([A-ZÁÉÍÓÖŐÚÜŰa-záéíóöőúüű]+\s+\d{1,2}\.)\s*\|', body_text)
                    date_str = date_match.group(1).strip() if date_match else f"{datetime.now().strftime('%b %d.')}"
                    
                    if not re.search(r'\d{4}', date_str):
                        date_str = f"{current_year}. {date_str}"

                    author = "Ismeretlen_Eloado"
                    author_locators = page.locator("h1:visible ~ h2, h1:visible + p, h1:visible ~ div p").all_inner_texts()
                    if author_locators:
                        for text in author_locators:
                            clean_text = text.strip()
                            if clean_text and "|" not in clean_text:
                                author = clean_text
                                break
                    
                    safe_date = sanitize_filename(date_str)
                    safe_title = sanitize_filename(title)
                    safe_author = sanitize_filename(author)
                    
                    filename = f"{safe_date}_{safe_author}_{safe_title}_{title_hash}.mp3"
                    filepath = os.path.join(args.out, filename)

                    print(f"Feldolgozás: {title}")

                    # --- JAVÍTÁS 2: Közvetlen letöltés az <audio> tagből ---
                    download_success = False
                    try:
                        # Megkeressük az audio taget és kinyerjük a forrás URL-t
                        audio_locator = page.locator("audio").first
                        audio_src = audio_locator.get_attribute("src")
                        
                        # Előfordulhat, hogy az src egy <source> tagen belül van
                        if not audio_src:
                            source_locator = audio_locator.locator("source").first
                            if source_locator.count() > 0:
                                audio_src = source_locator.get_attribute("src")

                        if audio_src:
                            if not audio_src.startswith("http"):
                                audio_src = urljoin(page.url, audio_src)
                            
                            print(f"Letöltés megkezdése a háttérben...")
                            download_file_with_requests(audio_src, filepath)
                            download_success = True
                        else:
                            print(f"❌ Nem találtam audio forrást: {title}")

                    except Exception as e:
                        print(f"❌ Hiba a letöltés során: {e}")

                    if download_success:
                        print(f"✅ Sikeres letöltés: {filename}")
                        with open(visited_file, 'a', encoding='utf-8') as f:
                            f.write(f"{title_hash}\n")
                        visited.add(title_hash)

                    # Visszalépés a listára
                    vissza_btn = page.locator("text=Vissza").first
                    if vissza_btn.is_visible():
                        vissza_btn.click()
                    else:
                        page.go_back()
                        
                except Exception as e:
                    print(f"❌ Hiba történt a(z) {i}. elem feldolgozásakor: {e}")
                    page.goto(url) 

        browser.close()
        print("\n🎉 Folyamat befejezve!")

if __name__ == "__main__":
    main()