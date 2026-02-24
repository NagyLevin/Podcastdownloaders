import argparse
import hashlib
import os
import re
import urllib.parse
from datetime import datetime
from urllib.parse import urljoin

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeoutError

def sanitize_filename(name: str) -> str:
    """Eltávolítja a fájlrendszer számára érvénytelen karaktereket."""
    return re.sub(r'[\\/*?:"<>|]', "", name).strip()

def truncate_text(text: str, max_length: int) -> str:
    """Levágja a szöveget a megadott hosszra."""
    if len(text) > max_length:
        return text[:max_length].strip() + "..."
    return text

def download_file_with_requests(url: str, filepath: str):
    """Közvetlenül letölti a fájlt az URL-ről, böngészőnek álcázva magát."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://podcast.hu/",
        "Accept": "audio/webm,audio/ogg,audio/wav,audio/*;q=0.9,application/ogg;q=0.7,video/*;q=0.6,*/*;q=0.5"
    }
    response = requests.get(url, stream=True, headers=headers)
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
            for line in f:
                line = line.strip()
                if line:
                    parts = line.split('\t')
                    visited.add(parts[0])

    current_year = datetime.now().year

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        for page_num in range(args.startpage, args.endpage + 1):
            url = f"https://podcast.hu/kereses?search=&type=episode&category=&duration=&page={page_num}"
            print(f"\n--- Oldal betöltése: {page_num}. oldal ---")
            
            try:
                page.goto(url)
            except Exception as e:
                print(f"❌ Hiba az oldal betöltésekor: {e}")
                continue
            
            # Sütik elfogadása
            try:
                page.wait_for_selector("text=Mindegyik elfogadása", timeout=3000)
                page.locator("text=Mindegyik elfogadása").click()
                print("✅ Süti banner leokézva.")
            except PwTimeoutError:
                pass
            
            try:
                page.wait_for_selector("a[href*='/podcast/']", timeout=10000)
            except PwTimeoutError:
                print(f"Nincsenek találatok a(z) {page_num}. oldalon, vagy a végére értünk.")
                break

            # URL-ek összegyűjtése
            episode_urls = []
            links = page.locator("a").all()
            for link in links:
                href = link.get_attribute("href")
                if href and "/podcast/" in href:
                    parts = [part for part in href.split("/") if part]
                    try:
                        podcast_idx = parts.index("podcast")
                        if len(parts) >= podcast_idx + 3: 
                            full_url = urljoin(page.url, href)
                            if full_url not in episode_urls:
                                episode_urls.append(full_url)
                    except ValueError:
                        continue
            
            if not episode_urls:
                print("Nem találtam letölthető epizódokat ezen az oldalon.")
                break

            print(f"Talált epizódok száma az oldalon: {len(episode_urls)}")

            for i, ep_url in enumerate(episode_urls):
                try:
                    # --- JAVÍTVA: Biztonságosabb betöltés, felkészülve a külsős letöltő linkekre ---
                    try:
                        page.goto(ep_url, timeout=15000)
                        page.wait_for_load_state("domcontentloaded", timeout=5000)
                    except Exception as e:
                        if ".m4a" in ep_url.lower() or ".mp3" in ep_url.lower() or "anchor.fm" in ep_url:
                            pass # Ha közvetlen link, a timeout ellenére is megpróbáljuk letölteni
                        else:
                            print(f"❌ Oldal betöltési hiba: {ep_url} ({e})")
                            continue

                    # Várunk a címre VAGY a böngésző beépített audio/video lejátszójára
                    try:
                        page.wait_for_selector("h1.p-episode__title--desktop, h1:visible, video, audio", timeout=5000)
                    except PwTimeoutError:
                        pass # Nem dobjuk el azonnal, hátha az URL maga a forrás
                        
                    title_loc = page.locator("h1.p-episode__title--desktop")
                    if title_loc.count() == 0:
                        title_loc = page.locator("h1:visible")
                        
                    date_str = ""
                    author = "Ismeretlen_Eloado"

                    # --- JAVÍTVA: Kétféle feldolgozási út (Normál oldal vs. Csupasz lejátszó) ---
                    if title_loc.count() > 0:
                        # 1. Normál podcast.hu dizájn
                        title = title_loc.first.inner_text().strip()
                        title_hash = hashlib.md5(title.encode('utf-8')).hexdigest()[:8]
                        
                        minutes_locator = page.locator(".p-episode__minutes").first
                        if minutes_locator.count() > 0:
                            date_text = minutes_locator.inner_text().strip()
                            if '|' in date_text:
                                date_str = date_text.split('|')[0].strip()
                            else:
                                date_str = date_text
                                
                        if len(date_str) > 20:
                            date_str = "" 

                        if not date_str:
                            body_text = page.locator("body").inner_text()
                            date_match = re.search(r'([A-ZÁÉÍÓÖŐÚÜŰa-záéíóöőúüű]+\s+\d{1,2}\.)\s*\|', body_text)
                            if date_match:
                                date_str = date_match.group(1).strip()
                                
                        author_locator = page.locator(".p-episode__author").first
                        if author_locator.count() > 0:
                            found_author = author_locator.inner_text().strip()
                            if found_author: 
                                author = found_author
                    else:
                        # 2. Csupasz lejátszó / Külsős link (Nincs h1)
                        decoded_url = urllib.parse.unquote(ep_url)
                        title_fallback = decoded_url.split("/")[-1].split("?")[0]
                        if not title_fallback or len(title_fallback) < 3:
                            title_fallback = f"Episode_{i}"
                            
                        title = "Kulsos_Epizod_" + title_fallback
                        # Itt az URL-ből generálunk hasht, hogy egyedi legyen
                        title_hash = hashlib.md5(ep_url.encode('utf-8')).hexdigest()[:8]
                        date_str = ""
                    
                    # Hash ellenőrzés (minden útvonalra érvényes)
                    if title_hash in visited:
                        print(f"Már letöltve, ugrás: {title}")
                        continue 

                    # Fájlnév formázása
                    safe_date = ""
                    if date_str:
                        if not re.search(r'\d{4}', date_str):
                            date_str = f"{current_year}. {date_str}"
                        safe_date = sanitize_filename(date_str) + "_"

                    safe_author = truncate_text(sanitize_filename(author), 50)
                    safe_title = truncate_text(sanitize_filename(title), 100) 
                    
                    print(f"Feldolgozás: {title} | Előadó: {author}")

                    # --- JAVÍTVA: Audio forrás kinyerése okosabban ---
                    download_success = False
                    try:
                        audio_src = ""
                        # Keresés <audio> tagben
                        if page.locator("audio").count() > 0:
                            audio_src = page.locator("audio").first.get_attribute("src")
                            if not audio_src and page.locator("audio source").count() > 0:
                                audio_src = page.locator("audio source").first.get_attribute("src")
                        
                        # Keresés <video> tagben (a Chromium gyakran ebbe teszi a nyers audio linkeket!)
                        if not audio_src and page.locator("video").count() > 0:
                            audio_src = page.locator("video").first.get_attribute("src")
                            if not audio_src and page.locator("video source").count() > 0:
                                audio_src = page.locator("video source").first.get_attribute("src")
                        
                        # Ha sehol nincs tag, de az URL eleve média
                        if not audio_src:
                            current_url = page.url.lower()
                            if ".m4a" in current_url or ".mp3" in current_url or "anchor.fm" in current_url:
                                audio_src = page.url
                            elif ".m4a" in ep_url.lower() or ".mp3" in ep_url.lower() or "anchor.fm" in ep_url.lower():
                                audio_src = ep_url

                        if audio_src:
                            if not audio_src.startswith("http"):
                                audio_src = urljoin(page.url, audio_src)
                            
                            # Kiterjesztés okos felismerése
                            ext = ".mp3"
                            if ".m4a" in audio_src.lower():
                                ext = ".m4a"
                                
                            filename = f"{safe_date}{safe_author}_{safe_title}_{title_hash}{ext}"
                            filepath = os.path.join(args.out, filename)

                            print(f"Letöltés megkezdése a háttérben ({ext})...")
                            download_file_with_requests(audio_src, filepath)
                            download_success = True
                        else:
                            print(f"❌ Nincs felismerhető audio lejátszó vagy link ezen az oldalon.")

                    except Exception as e:
                        print(f"❌ Hiba a letöltés során: {e}")

                    # Naplózás
                    if download_success:
                        print(f"✅ Sikeres letöltés: {filename}")
                        with open(visited_file, 'a', encoding='utf-8') as f:
                            f.write(f"{title_hash}\t{title}\n")
                        visited.add(title_hash)
                        
                except Exception as e:
                    print(f"❌ Hiba történt az epizód feldolgozásakor: {e}")

        browser.close()
        print("\n🎉 Folyamat befejezve!")

if __name__ == "__main__":
    main()