import yt_dlp
import argparse
import os
import sys

def letolto(fajl_nev, kimeneti_mappa):
    # 1. Ellenőrizzük, létezik-e az input fájl
    if not os.path.exists(fajl_nev):
        print(f"[-] Hiba: A '{fajl_nev}' fájl nem található!")
        return

    # 2. Mappa létrehozása, ha még nem létezik
    if not os.path.exists(kimeneti_mappa):
        os.makedirs(kimeneti_mappa)
        print(f"[+] Mappa létrehozva: {kimeneti_mappa}")

    # 3. yt-dlp konfiguráció
    ydl_opts = {
        # Csak a legjobb hangot keresse
        'format': 'bestaudio/best',
        
        # Mentési hely és fájlnév (Mappa/Cím.mp3)
        'outtmpl': f'{kimeneti_mappa}/%(title)s.%(ext)s',
        
        'noplaylist': False,  # Playlisteket is töltsön le
        
        # FFmpeg beállítások az MP3-hoz
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        
        # Megmondjuk a programnak, hogy a script mellett is keresse az ffmpeg.exe-t
        'ffmpeg_location': os.getcwd(),
        
        # Tisztítás: ne hagyja ott az eredeti .opus vagy .webm fájlt
        'keepvideo': False,
    }

    # 4. Linkek beolvasása
    with open(fajl_nev, 'r', encoding='utf-8') as f:
        linkek = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    if not linkek:
        print("[!] A fájl üres, nincs mit letölteni.")
        return

    print(f"[*] Indítás... Összesen {len(linkek)} tétel.")
    print(f"[*] Mentési hely: {os.path.abspath(kimeneti_mappa)}\n")

    # 5. Letöltési folyamat
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        for link in linkek:
            try:
                print(f"\n[>>>] Letöltés: {link}")
                ydl.download([link])
            except Exception as e:
                print(f"[!] Hiba történt ennél a linknél: {link}")
                print(f"    Részletek: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Profi MP3 Letöltő (yt-dlp alapú)")
    parser.add_argument('--input', type=str, default='inputok.txt', help='A linkeket tartalmazó fájl')
    parser.add_argument('--output', type=str, default='soundcloud_letoltesek', help='A mappa neve')
    
    args = parser.parse_args()
    
    try:
        letolto(args.input, args.output)
        print("\n[OK] Minden feladat befejezve.")
    except KeyboardInterrupt:
        print("\n[!] Megszakítva a felhasználó által.")
        sys.exit()