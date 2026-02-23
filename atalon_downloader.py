import requests
import re
import os
import json

def download_atalon_podcast_v3(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
    }

    try:
        print(f"Oldal elemzése: {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # 1. Trükk: A Next.js az adatokat egy <script id="__NEXT_DATA__"> tag-be rejti
        # Megkeressük ezt a JSON blokkot a HTML forrásban
        json_pattern = r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>'
        match = re.search(json_pattern, response.text)

        audio_url = None
        title = "atalon_podcast"

        if match:
            data = json.loads(match.group(1))
            # Itt egy mély keresést végzünk a JSON-ben az audioUrl kulcsra
            # Az Atalon struktúrája szerint ez általában a 'queries' vagy 'props' alatt van
            json_str = match.group(1)
            # Egyszerűbb regexes keresés a JSON-ben a közvetlen URL-re
            url_match = re.search(r'https://[^\s"<>]*?\.(?:mp3|m4a|wav|aac)[^\s"<>]*', json_str)
            
            if url_match:
                audio_url = url_match.group(0).replace('\\u0026', '&') # Tisztítjuk az URL-t

        # 2. Ha a Next.js adatban nem volt meg, hátha mégis ott van a forrásban
        if not audio_url:
            audio_extensions = ('.mp3', '.m4a', '.wav', '.aac')
            # Minden olyan szöveget keresünk, ami https-sel kezdődik és audio kiterjesztésre végződik
            links = re.findall(r'https?://[^\s"<>]*?\.(?:mp3|m4a|wav|aac)', response.text)
            if links:
                audio_url = links[0]

        if not audio_url:
            print("Sajnos így sem találtam meg a közvetlen linket. Az oldal dinamikusan töltheti be.")
            return

        # Fájlnév generálása
        clean_title = re.sub(r'[\\/*?:"<>|]', "", url.split('/')[-1]) or "podcast_adas"
        ext = ".mp3"
        for e in ['.m4a', '.wav', '.aac']:
            if e in audio_url.lower():
                ext = e
        
        filename = f"{clean_title}{ext}"

        # Letöltés
        print(f"Talált link: {audio_url}")
        print(f"Letöltés indítása: {filename}...")
        
        with requests.get(audio_url, stream=True, headers=headers) as r:
            r.raise_for_status()
            with open(filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024): # 1MB-os darabok
                    f.write(chunk)
        
        print(f"KÉSZ! Mentve: {os.path.abspath(filename)}")

    except Exception as e:
        print(f"Hiba: {e}")

if __name__ == "__main__":
    link = input("Add meg az Atalon podcast oldal linkjét: ").strip()
    download_atalon_podcast_v3(link)