import argparse
import hashlib
import os
import re
import time
import unicodedata
from datetime import datetime
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright, TimeoutError as PwTimeoutError


def sanitize_filename(name: str) -> str:
    """Eltávolítja a fájlrendszer számára érvénytelen karaktereket."""
    return re.sub(r'[\\/*?:"<>|]', "", name).strip()


def truncate_text(text: str, max_length: int) -> str:
    """Levágja a szöveget a megadott hosszra."""
    if len(text) > max_length:
        return text[:max_length].strip() + "..."
    return text


def strip_accents(s: str) -> str:
    """Ékezetek leszedése (á->a, ő->o, stb.)"""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))


MONTH_MAP = {
    # Hungarian/English (accentless)
    "jan": 1, "januar": 1, "january": 1,
    "feb": 2, "febr": 2, "februar": 2, "february": 2,
    "mar": 3, "marc": 3, "marcius": 3, "march": 3,
    "apr": 4, "april": 4, "aprilis": 4,
    "maj": 5, "majus": 5, "may": 5,
    "jun": 6, "juni": 6, "june": 6,
    "jul": 7, "juli": 7, "july": 7,
    "aug": 8, "augusztus": 8, "august": 8,
    "szept": 9, "szep": 9, "szeptember": 9, "sep": 9, "sept": 9, "september": 9,
    "okt": 10, "oktober": 10, "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


def parse_month_from_date_str(date_str: str) -> int | None:
    """
    Visszaadja a hónap számát (1..12), ha felismerhető.
    Támogatja pl:
      - "ápr. 1."
      - "Apr 01."
      - "2026. ápr. 01."
      - "2026. 04. 01."
    """
    if not date_str:
        return None

    s = date_str.strip()

    # Levágjuk az elejéről az évet, ha van: "2026. ..."
    s = re.sub(r"^\s*\d{4}\.\s*", "", s)

    # 1) Numerikus hónap forma: "04. 01." vagy "4. 1."
    m_num = re.search(r"^\s*(\d{1,2})\.\s*\d{1,2}\.", s)
    if m_num:
        mn = int(m_num.group(1))
        if 1 <= mn <= 12:
            return mn

    # 2) Szöveges hónap forma
    token_match = re.search(r"([A-Za-zÁÉÍÓÖŐÚÜŰáéíóöőúüű]+)\.?\s+\d{1,2}", s)
    if not token_match:
        token_match = re.search(r"\b([A-Za-zÁÉÍÓÖŐÚÜŰáéíóöőúüű]+)\.?\b", s)

    if token_match:
        raw = token_match.group(1)
        key = strip_accents(raw.lower()).replace(".", "").strip()
        if key in MONTH_MAP:
            return MONTH_MAP[key]
        for k, v in MONTH_MAP.items():
            if key.startswith(k):
                return v

    return None


def index_downloaded_files(out_dir: str) -> dict[str, list[str]]:
    """
    Kimeneti mappa fájljait indexeli title_hash alapján.
    Visszatér: hash -> [teljes elérési utak]
    """
    by_hash: dict[str, list[str]] = {}
    if not os.path.isdir(out_dir):
        return by_hash

    for fn in os.listdir(out_dir):
        full = os.path.join(out_dir, fn)
        if not os.path.isfile(full):
            continue

        # A te formátumod: ..._{8hex}.mp3 vagy ..._{8hex}.m4a
        m = re.search(r"_([0-9a-fA-F]{8})(\.[A-Za-z0-9]+)$", fn)
        if not m:
            continue
        h = m.group(1).lower()
        by_hash.setdefault(h, []).append(full)

    return by_hash


def has_leading_year(filename: str) -> bool:
    """Igaz, ha a fájlnév ELEJÉN év van: '2026. ...' / '2026 ...' / '2026_...'"""
    return re.match(r"^(\d{4})(?=[\.\s_])", filename) is not None


def apply_or_insert_year_prefix(filename: str, new_year: int, sep: str = ". ") -> str:
    """
    - Ha a fájlnév ELEJÉN van év (YYYY...), akkor azt cseréli new_year-re.
    - Ha NINCS, akkor beszúrja elé: f"{new_year}{sep}{filename}"
    """
    m = re.match(r"^(\d{4})(?=[\.\s_])", filename)
    if m:
        old_year = m.group(1)
        if old_year == str(new_year):
            return filename
        return re.sub(r"^(\d{4})(?=[\.\s_])", str(new_year), filename, count=1)

    return f"{new_year}{sep}{filename}"


def make_unique_path(path: str) -> str:
    """Ha létezik a célfájl, sorszámozva egyedi nevet csinál."""
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    i = 1
    while True:
        candidate = f"{base}__renamed{i}{ext}"
        if not os.path.exists(candidate):
            return candidate
        i += 1


def main():
    parser = argparse.ArgumentParser(description="Podcast fájlnevek év-javítása (letöltés nélkül)")
    parser.add_argument("--startpage", type=int, default=1, help="Kezdő oldal száma")
    parser.add_argument("--endpage", type=int, default=100, help="Utolsó oldal száma")
    parser.add_argument("--out", type=str, default="./podcasts", help="Kimeneti mappa (ahol a fájlok vannak)")
    parser.add_argument("--headless", action="store_true", help="Headless futás")
    parser.add_argument("--startyear", type=int, default=datetime.now().year, help="Kezdő év (pl. 2026)")
    parser.add_argument("--dry-run", action="store_true", help="Csak kiírja mit nevezne át, nem módosít fájlokat")
    parser.add_argument("--year-sep", type=str, default=". ", help="Ha nincs év a fájlnév elején, ezzel szúrjuk be elé (pl '. ' vagy '._').")
    args = parser.parse_args()

    downloaded_by_hash = index_downloaded_files(args.out)
    total_local = sum(len(v) for v in downloaded_by_hash.values())
    print(f"[i] Output mappa indexelve: {len(downloaded_by_hash)} hash, összes fájl: {total_local} db")

    current_year = args.startyear
    prev_month = None

    scanned_eps = 0
    matched_local = 0
    renamed = 0
    inserted_year_prefix = 0
    replaced_year_prefix = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        context = browser.new_context()
        page = context.new_page()

        cookie_done = False

        for page_num in range(args.startpage, args.endpage + 1):
            url = f"https://podcast.hu/kereses?search=&type=episode&category=&duration=&page={page_num}"
            print(f"\n--- Oldal: {page_num} ---")

            try:
                page.goto(url, timeout=20000)
            except Exception as e:
                print(f":( Hiba az oldal betöltésekor: {e}")
                continue

            # Sütik elfogadása (csak egyszer próbáljuk agresszívan)
            if not cookie_done:
                try:
                    page.wait_for_selector("text=Mindegyik elfogadása", timeout=3000)
                    page.locator("text=Mindegyik elfogadása").click()
                    cookie_done = True
                    print("[i] Süti banner elfogadva.")
                except PwTimeoutError:
                    cookie_done = True

            try:
                page.wait_for_selector("a[href*='/podcast/']", timeout=10000)
            except PwTimeoutError:
                print("[i] Nincs több találat, vagy a végére értünk.")
                break

            episode_urls = []
            links = page.locator("a").all()
            for link in links:
                href = link.get_attribute("href")
                if not href:
                    continue
                if href.startswith("/podcast/") or href.startswith("https://podcast.hu/podcast/"):
                    clean_href = href.split("?")[0]
                    parts = [part for part in clean_href.split("/") if part]
                    try:
                        podcast_idx = parts.index("podcast")
                        if len(parts) >= podcast_idx + 3:
                            full_url = urljoin(page.url, href)
                            if full_url not in episode_urls:
                                episode_urls.append(full_url)
                    except ValueError:
                        continue

            if not episode_urls:
                print("[i] Nem találtam epizód URL-eket ezen az oldalon.")
                continue

            print(f"[i] Epizód URL-ek: {len(episode_urls)} db")

            for ep_url in episode_urls:
                scanned_eps += 1

                # minimal retry a betöltéshez
                ok = False
                for attempt in range(1, 4):
                    try:
                        page.goto(ep_url, timeout=20000)
                        page.wait_for_load_state("domcontentloaded", timeout=7000)
                        ok = True
                        break
                    except Exception as e:
                        if attempt < 3:
                            print(f"  [!] Betöltési hiba ({attempt}/3): {e} -> retry 2s")
                            time.sleep(2)
                        else:
                            print(f"  [x] Végleges betöltési hiba: {ep_url} ({e})")
                if not ok:
                    continue

                # Cím
                title_loc = page.locator("h1.p-episode__title--desktop")
                if title_loc.count() == 0:
                    title_loc = page.locator("h1:visible")

                if title_loc.count() == 0:
                    continue

                title = title_loc.first.inner_text().strip()
                title_hash = hashlib.md5(title.encode("utf-8")).hexdigest()[:8].lower()

                # Dátum szöveg
                date_str = ""
                minutes_locator = page.locator(".p-episode__minutes").first
                if minutes_locator.count() > 0:
                    date_text = minutes_locator.inner_text().strip()
                    if "|" in date_text:
                        date_str = date_text.split("|")[0].strip()
                    else:
                        date_str = date_text

                # Hónap kinyerése + évhatár (CSAK január -> december)
                month_num = parse_month_from_date_str(date_str)
                if month_num is not None:
                    if prev_month is not None and prev_month == 1 and month_num == 12:
                        current_year -= 1
                        print(f"[i] Évhatár észlelve (hónap {prev_month} -> {month_num}), új év: {current_year}")
                    prev_month = month_num

                desired_year = current_year

                # Ha a fájl megvan helyben, javítjuk / beszúrjuk az évet
                if title_hash in downloaded_by_hash:
                    matched_local += 1
                    paths = downloaded_by_hash[title_hash]

                    for old_path in list(paths):
                        old_fn = os.path.basename(old_path)

                        had_year = has_leading_year(old_fn)
                        new_fn = apply_or_insert_year_prefix(old_fn, desired_year, sep=args.year_sep)

                        if new_fn == old_fn:
                            continue

                        new_path = os.path.join(args.out, new_fn)
                        new_path = make_unique_path(new_path)

                        tag = "REPLACE-YEAR" if had_year else "INSERT-YEAR"
                        print(f"[{tag}] {old_fn}  ->  {os.path.basename(new_path)}")

                        if not args.dry_run:
                            os.rename(old_path, new_path)
                            renamed += 1
                            if had_year:
                                replaced_year_prefix += 1
                            else:
                                inserted_year_prefix += 1

                            # frissítjük az indexet
                            paths.remove(old_path)
                            paths.append(new_path)

        browser.close()

    print("\n" + "=" * 50)
    print("ÖSSZESÍTŐ")
    print(f"Bejárt epizódok: {scanned_eps} db")
    print(f"Helyben megtalált (hash egyező): {matched_local} db")
    if args.dry_run:
        print("Átnevezések: DRY-RUN (nem történt tényleges átnevezés)")
    else:
        print(f"Átnevezett fájlok: {renamed} db")
        print(f" - Év csere (volt év a elején): {replaced_year_prefix} db")
        print(f" - Év beszúrás (nem volt év a elején): {inserted_year_prefix} db")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    main()
    #python tele_fix_years.py --startpage 1 --endpage 100 --out ./podcasts --startyear 2026 --headless --dry-run