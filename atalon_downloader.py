import argparse
import json
import os
import re
from urllib.parse import urlparse

import requests
from playwright.sync_api import sync_playwright


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}

# Atalon epizód linkek tipikusan így néznek ki: /valami-s01-e14
EPISODE_PATH_RE = re.compile(r"-s\d+-e\d+", re.IGNORECASE)

AUDIO_URL_RE = re.compile(
    r'https://[^\s"<>]*?\.(?:mp3|m4a|wav|aac)[^\s"<>]*',
    re.IGNORECASE
)


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def read_input_links(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        lines = [ln.strip() for ln in f.readlines()]
    out = []
    seen = set()
    for ln in lines:
        if not ln or ln.startswith("#"):
            continue
        if ln not in seen:
            seen.add(ln)
            out.append(ln)
    return out


def sanitize_filename(name: str) -> str:
    name = re.sub(r"[\\/*?\"<>|:]", "", name).strip()
    name = re.sub(r"\s+", " ", name)
    return name or "podcast_adas"


def extension_from_audio_url(audio_url: str) -> str:
    lower = audio_url.lower()
    for ext in [".mp3", ".m4a", ".wav", ".aac"]:
        if ext in lower:
            return ext
    return ".mp3"


def extract_audio_url_requests(html: str) -> str | None:
    # 1) __NEXT_DATA__ blokk
    m = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
        flags=re.DOTALL
    )
    if m:
        json_str = m.group(1)
        url_match = AUDIO_URL_RE.search(json_str)
        if url_match:
            return url_match.group(0).replace("\\u0026", "&")

    # 2) fallback: sima HTML-ben keresés
    url_match = AUDIO_URL_RE.search(html)
    if url_match:
        return url_match.group(0).replace("\\u0026", "&")

    return None


def download_atalon_episode_requests(url: str, out_dir: str, session: requests.Session) -> None:
    print(f"Epizód elemzése: {url}")
    r = session.get(url, headers=DEFAULT_HEADERS, timeout=30)
    r.raise_for_status()

    audio_url = extract_audio_url_requests(r.text)
    if not audio_url:
        print("  - Nem találtam audio URL-t az epizód oldalon (__NEXT_DATA__/HTML).")
        return

    # fájlnév: slug + kiterjesztés (ugyanaz a logika, mint nálad)
    slug = url.rstrip("/").split("/")[-1] or "podcast_adas"
    slug = sanitize_filename(slug)
    ext = extension_from_audio_url(audio_url)
    filename = f"{slug}{ext}"
    out_path = os.path.join(out_dir, filename)

    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        print(f"  - Már létezik, kihagyom: {out_path}")
        return

    print(f"  - Talált link: {audio_url}")
    print(f"  - Letöltés: {out_path}")

    with session.get(audio_url, stream=True, headers=DEFAULT_HEADERS, timeout=120) as rr:
        rr.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in rr.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    print("  - KÉSZ")


def is_episode_url(u: str) -> bool:
    p = urlparse(u)
    return bool(EPISODE_PATH_RE.search(p.path or ""))


def collect_episode_links_via_dom(show_url: str, headful: bool = False, scroll_rounds: int = 8) -> list[str]:
    """
    Megnyitja a show oldalt Playwright-tal, majd a DOM-ból kiveszi az összes <a href> linket,
    és az epizód-szerűeket visszaadja.
    Ez gyakorlatilag ugyanaz, mint a 'Link címének másolása'.
    """
    out = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headful)
        context = browser.new_context()
        page = context.new_page()
        page.goto(show_url, wait_until="domcontentloaded", timeout=60000)

        # Kicsit várunk + görgetünk, hogy betöltse az epizódlistát (ha lazy load)
        page.wait_for_timeout(1500)
        for _ in range(max(0, scroll_rounds)):
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(600)

        base_netloc = urlparse(show_url).netloc
        hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")

        seen = set()
        for h in hrefs:
            if not isinstance(h, str):
                continue
            pu = urlparse(h)
            if pu.scheme not in ("http", "https"):
                continue
            if pu.netloc != base_netloc:
                continue
            if is_episode_url(h):
                if h not in seen:
                    seen.add(h)
                    out.append(h)

        page.close()
        browser.close()

    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Bemeneti txt (soronként 1 link).")
    ap.add_argument("--out", default="atalon", help="Kimeneti mappa (default: atalon).")
    ap.add_argument("--headful", action="store_true", help="Lásd a böngészőt (debug).")
    ap.add_argument("--scroll", type=int, default=8, help="Hány görgetés a show oldalon (lazy load miatt).")
    args = ap.parse_args()

    ensure_dir(args.out)
    seeds = read_input_links(args.input)
    if not seeds:
        print("Nincs link az inputban.")
        return

    # 1) Seedekből epizód linkek kigyűjtése
    episode_links = []
    seen_eps = set()

    for u in seeds:
        if is_episode_url(u):
            if u not in seen_eps:
                seen_eps.add(u)
                episode_links.append(u)
            continue

        print(f"Show oldal feldolgozása (epizód linkek kigyűjtése): {u}")
        eps = collect_episode_links_via_dom(u, headful=args.headful, scroll_rounds=args.scroll)
        print(f"  - Talált epizód linkek: {len(eps)}")
        for e in eps:
            if e not in seen_eps:
                seen_eps.add(e)
                episode_links.append(e)

    if not episode_links:
        print("Nem találtam epizód linkeket.")
        return

    print(f"\nÖsszes epizód letöltésre: {len(episode_links)}")
    print(f"Kimeneti mappa: {os.path.abspath(args.out)}\n")

    # 2) Letöltés: a te eredeti requests + __NEXT_DATA__ logikáddal
    with requests.Session() as session:
        for i, ep in enumerate(episode_links, 1):
            print(f"=== ({i}/{len(episode_links)}) ===")
            try:
                download_atalon_episode_requests(ep, args.out, session)
            except Exception as e:
                print(f"  - Hiba: {e}")

    print("\nMinden kész.")


if __name__ == "__main__":
    main()