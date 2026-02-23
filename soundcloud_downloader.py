#!/usr/bin/env python3
import argparse
import os
import re
from pathlib import Path
from datetime import datetime, timezone

import yt_dlp

DATE_YYYYMMDD = re.compile(r"^\d{8}$")

def to_yyyy_mm_dd_from_yyyymmdd(s: str | None) -> str | None:
    if not s: return None
    s = str(s).strip()
    if DATE_YYYYMMDD.match(s):
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return None

def to_yyyy_mm_dd_from_timestamp(ts) -> str | None:
    if ts is None: return None
    try:
        dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None

def make_date_prefix(info: dict) -> str:
    d = to_yyyy_mm_dd_from_yyyymmdd(info.get("upload_date"))
    if d: return d
    d = to_yyyy_mm_dd_from_yyyymmdd(info.get("release_date"))
    if d: return d
    d = to_yyyy_mm_dd_from_timestamp(info.get("timestamp"))
    if d: return d
    return "UnknownDate"

def safe_rename(src: Path, dst: Path) -> Path:
    if not dst.exists():
        src.rename(dst)
        return dst
    parent = dst.parent
    stem = dst.stem
    suffix = dst.suffix
    i = 2
    while True:
        candidate = parent / f"{stem} ({i}){suffix}"
        if not candidate.exists():
            src.rename(candidate)
            return candidate
        i += 1

def mark_link_as_done(fajl_nev: str, link: str) -> None:
    """Kicseréli a linket a fájlban '# link' formátumra."""
    path = Path(fajl_nev)
    lines = path.read_text(encoding="utf-8").splitlines()
    
    new_lines = []
    for line in lines:
        # Ha pont ezt a linket találjuk meg (tisztítva), elérakjuk a #-et
        if line.strip() == link:
            new_lines.append(f"# {line}")
        else:
            new_lines.append(line)
    
    path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")

def letolto(fajl_nev: str, kimeneti_mappa: str) -> None:
    fajl_path = Path(fajl_nev)
    out_dir = Path(kimeneti_mappa)

    if not fajl_path.exists():
        print(f"[-] Hiba: A '{fajl_nev}' fájl nem található!")
        return

    out_dir.mkdir(parents=True, exist_ok=True)

    def rename_hook(d: dict) -> None:
        if d.get("status") != "finished":
            return
        filename = d.get("filename")
        info = d.get("info_dict") or {}
        if not filename: return
        src = Path(filename)
        if not src.exists(): return
        date_prefix = make_date_prefix(info)
        dst = src.with_name(f"{date_prefix} - {src.name}")
        if dst.name == src.name: return
        try:
            new_path = safe_rename(src, dst)
            print(f"[i] Átnevezve: {new_path.name}")
        except Exception as e:
            print(f"[!] Átnevezés hiba: {e}")

    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": str(out_dir / "%(title)s.%(ext)s"),
        "noplaylist": False,
        "restrictfilenames": True,
        "windowsfilenames": True,
        "progress_hooks": [rename_hook],
    }

    # Linkek beolvasása (kihagyva az üreseket és a #-el kezdődőket)
    linkek = []
    with fajl_path.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            linkek.append(raw)

    if not linkek:
        print("[!] Nincsenek letöltendő (nem kommentelt) linkek a fájlban.")
        return

    print(f"[*] {len(linkek)} letöltendő link található.")

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        for link in linkek:
            try:
                print(f"\n>>> Feldolgozás: {link}")
                # A download metódus 0-t ad vissza siker esetén
                result = ydl.download([link])
                
                # Ha nem dobott hibát, sikeresnek vesszük és kipipáljuk
                print(f"[+] Sikeres letöltés, link megjelölése készként...")
                mark_link_as_done(fajl_nev, link)
                
            except Exception as e:
                print(f"[!] Hiba a letöltés során ({link}): {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="inputok.txt")
    parser.add_argument("--output", type=str, default="soundcloud_letoltesek")
    args = parser.parse_args()

    letolto(args.input, args.output)
    print("\n[OK] Minden folyamat befejeződött.")

#python3 soundcloud_downloader.py --input inputok.txt --output soundcloud_letoltesek