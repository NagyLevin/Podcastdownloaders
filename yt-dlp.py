import yt_dlp
import argparse
import os

def letolto(fajl_nev, kimeneti_mappa):
    if not os.path.exists(fajl_nev):
        print(f"[-] Hiba: A '{fajl_nev}' fájl nem található!")
        return

    if not os.path.exists(kimeneti_mappa):
        os.makedirs(kimeneti_mappa)

    ydl_opts = {
        # Csak a legjobb hangot szedjük le, de nem nyúlunk hozzá
        'format': 'bestaudio/best',
        
        # Mentési hely: Mappa/Cím.eredeti_kiterjesztés
        'outtmpl': f'{kimeneti_mappa}/%(title)s.%(ext)s',
        
        'noplaylist': False,
        
        # Semmi utómunka, így nem keresi az FFmpeg-et
    }

    with open(fajl_nev, 'r', encoding='utf-8') as f:
        linkek = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    if not linkek:
        print("[!] Nincsenek linkek a fájlban.")
        return

    print(f"[*] Letöltés indul (eredeti formátumban)...")

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        for link in linkek:
            try:
                print(f"\n>>> {link}")
                ydl.download([link])
            except Exception as e:
                print(f"[!] Hiba: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str, default='inputok.txt')
    parser.add_argument('--output', type=str, default='soundcloud_letoltesek')
    args = parser.parse_args()
    
    letolto(args.input, args.output)
    print("\n[OK] Kész.")