import yt_dlp
import argparse
import os

def letolto(fajl_nev):
    # Ellenőrizzük, létezik-e a fájl
    if not os.path.exists(fajl_nev):
        print(f"Hiba: A '{fajl_nev}' fájl nem található!")
        return

    ydl_opts = {
        'format': 'bestvideo+bestaudio/best',
        'outtmpl': '%(title)s.%(ext)s',
        'noplaylist': False,
        'merge_output_format': 'mp4',
        'quiet': False, # Lássuk a folyamatot
    }

    # Linkek beolvasása a fájlból
    with open(fajl_nev, 'r', encoding='utf-8') as f:
        linkek = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    if not linkek:
        print("A fájl üres, nincs mit letölteni.")
        return

    print(f"{len(linkek)} link feldolgozása kezdődik...\n")

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        for link in linkek:
            try:
                print(f"--- Letöltés alatt: {link} ---")
                ydl.download([link])
            except Exception as e:
                print(f"Hiba történt a {link} letöltésekor: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tömeges videó letöltő")
    parser.add_argument('--input', type=str, default='inputok.txt', help='A linkeket tartalmazó fájl neve')
    
    args = parser.parse_args()
    letolto(args.input)