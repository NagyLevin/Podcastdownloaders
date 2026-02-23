#!/usr/bin/env python3
import argparse
import os
import re
from pathlib import Path
from datetime import datetime, timezone

import yt_dlp

DATE_YYYYMMDD = re.compile(r"^\d{8}$")


def to_yyyy_mm_dd_from_yyyymmdd(s: str | None) -> str | None:
    """Convert 'YYYYMMDD' -> 'YYYY-MM-DD'."""
    if not s:
        return None
    s = str(s).strip()
    if DATE_YYYYMMDD.match(s):
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return None


def to_yyyy_mm_dd_from_timestamp(ts) -> str | None:
    """Convert unix timestamp -> 'YYYY-MM-DD' (UTC)."""
    if ts is None:
        return None
    try:
        dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def make_date_prefix(info: dict) -> str:
    """
    Prefer upload_date (YYYYMMDD).
    Fallbacks: release_date (if present), then timestamp.
    """
    d = to_yyyy_mm_dd_from_yyyymmdd(info.get("upload_date"))
    if d:
        return d

    d = to_yyyy_mm_dd_from_yyyymmdd(info.get("release_date"))
    if d:
        return d

    d = to_yyyy_mm_dd_from_timestamp(info.get("timestamp"))
    if d:
        return d

    return "UnknownDate"


def safe_rename(src: Path, dst: Path) -> Path:
    """Rename, but if destination exists, append ' (2)', ' (3)', ..."""
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


def letolto(fajl_nev: str, kimeneti_mappa: str) -> None:
    # Jog / ToS: csak olyan tartalmat tölts le, amihez van engedélyed.

    fajl_path = Path(fajl_nev)
    out_dir = Path(kimeneti_mappa)

    if not fajl_path.exists():
        print(f"[-] Hiba: A '{fajl_nev}' fájl nem található!")
        return

    out_dir.mkdir(parents=True, exist_ok=True)

    def rename_hook(d: dict) -> None:
        # A hook a letöltés végén fut le.
        if d.get("status") != "finished":
            return

        filename = d.get("filename")
        info = d.get("info_dict") or {}

        if not filename:
            return

        src = Path(filename)
        if not src.exists():
            return

        date_prefix = make_date_prefix(info)

        # Új név: "YYYY-MM-DD - eredeti_fajlnev.ext"
        dst = src.with_name(f"{date_prefix} - {src.name}")

        # Ha valamiért már így néz ki, ne csináljunk semmit
        if dst.name == src.name:
            return

        try:
            new_path = safe_rename(src, dst)
            print(f"[i] Átnevezve: {new_path.name}")
        except Exception as e:
            print(f"[!] Átnevezés hiba: {e}")

    ydl_opts = {
        # Legjobb elérhető hang
        "format": "bestaudio/best",

        # Először simán cím.ext néven mentünk, majd a hook átnevezi dátum + cím.ext-re
        "outtmpl": str(out_dir / "%(title)s.%(ext)s"),

        # Ha playlist linket adsz meg, azt is engedi
        "noplaylist": False,

        # Fájlnév tisztítás (különösen Windows-on hasznos)
        "restrictfilenames": True,
        "windowsfilenames": True,

        # Letöltés utáni átnevezés
        "progress_hooks": [rename_hook],

        # (Opcionális) kevesebb log:
        # "quiet": True,
        # "no_warnings": True,
    }

    with fajl_path.open("r", encoding="utf-8") as f:
        linkek = []
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            if raw.lstrip().startswith("#"):
                continue
            linkek.append(raw)

    if not linkek:
        print("[!] Nincsenek linkek a fájlban.")
        return

    print("[*] Letöltés indul (eredeti formátumban, feltöltési dátum előtaggal)...")

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        for link in linkek:
            try:
                print(f"\n>>> {link}")
                ydl.download([link])
            except Exception as e:
                print(f"[!] Hiba: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="inputok.txt")
    parser.add_argument("--output", type=str, default="soundcloud_letoltesek")
    args = parser.parse_args()

    letolto(args.input, args.output)
    print("\n[OK] Kész.")

#python3 soundcloud_downloader.py --input inputok.txt --output soundcloud_letoltesek