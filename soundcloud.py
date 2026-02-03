#!/usr/bin/env python3
import argparse
import json
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

SC_BASE = "https://soundcloud.com"

SETS_RE = re.compile(r"^/[^/]+/sets/[^/?#]+$")


def normalize_url(url: str) -> str:
    """Strip query/fragment for stable storage."""
    p = urlparse(url)
    return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))


def ensure_out(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)


def load_seen(path: Path) -> set[str]:
    if path.exists():
        return set(json.loads(path.read_text(encoding="utf-8")))
    return set()


def save_seen(path: Path, seen: set[str]) -> None:
    path.write_text(json.dumps(sorted(seen), ensure_ascii=False, indent=2), encoding="utf-8")


def extract_from_html(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if SETS_RE.match(href):
            urls.add(normalize_url(urljoin(SC_BASE, href)))

    return sorted(urls)


def collect_static(search_url: str, timeout: int = 30) -> list[str]:
    r = requests.get(
        search_url,
        timeout=timeout,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; sc-playlist-link-saver/1.0)",
            "Accept-Language": "en-US,en;q=0.9,hu;q=0.8",
        },
    )
    r.raise_for_status()
    return extract_from_html(r.text)


def collect_browser(search_url: str, max_scrolls: int, settle_rounds: int, headful: bool) -> list[str]:
    # Playwright import here so static mode works without it installed
    from playwright.sync_api import sync_playwright

    found = set()

    def pull_links(page):
        # grab all anchors with /sets/ in href, then filter by regex
        hrefs = page.eval_on_selector_all(
            "a[href*='/sets/']",
            "els => els.map(e => e.getAttribute('href')).filter(Boolean)"
        )
        for href in hrefs:
            href = href.strip()
            if SETS_RE.match(href):
                found.add(normalize_url(urljoin(SC_BASE, href)))

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headful)
        page = browser.new_page()
        page.goto(search_url, wait_until="domcontentloaded", timeout=60000)

        # Try to close/accept common popups if present (best-effort, safe to ignore failures)
        for txt in ["Accept", "I accept", "Agree", "OK"]:
            try:
                page.get_by_role("button", name=txt).click(timeout=1500)
                break
            except Exception:
                pass

        # Initial pull
        pull_links(page)

        stable = 0
        last_count = len(found)

        for _ in range(max_scrolls):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1200)  # allow new items to load
            pull_links(page)

            if len(found) == last_count:
                stable += 1
            else:
                stable = 0
                last_count = len(found)

            if stable >= settle_rounds:
                break

        browser.close()

    return sorted(found)


def main():
    ap = argparse.ArgumentParser(description="Save SoundCloud playlist links from a search/sets page.")
    ap.add_argument("--url", required=True, help="SoundCloud search/sets URL")
    ap.add_argument("--out", required=True, help="Output folder")
    ap.add_argument("--mode", choices=["static", "browser"], default="browser",
                    help="static = parse HTML only, browser = scroll with Playwright (recommended)")
    ap.add_argument("--max-scrolls", type=int, default=80, help="Browser mode: max scroll attempts")
    ap.add_argument("--settle-rounds", type=int, default=3, help="Browser mode: stop after N no-growth rounds")
    ap.add_argument("--headful", action="store_true", help="Browser mode: show the browser window (debug)")
    args = ap.parse_args()

    out_dir = Path(args.out).expanduser().resolve()
    ensure_out(out_dir)

    seen_path = out_dir / "seen_playlists.json"
    txt_path = out_dir / "playlists.txt"
    json_path = out_dir / "playlists.json"

    seen = load_seen(seen_path)

    if args.mode == "static":
        urls = collect_static(args.url)
    else:
        urls = collect_browser(args.url, args.max_scrolls, args.settle_rounds, args.headful)

    new_urls = [u for u in urls if u not in seen]

    # update seen
    for u in urls:
        seen.add(u)

    # save outputs
    txt_path.write_text("\n".join(urls) + ("\n" if urls else ""), encoding="utf-8")
    json_path.write_text(json.dumps(urls, ensure_ascii=False, indent=2), encoding="utf-8")
    save_seen(seen_path, seen)

    print(f"Total found this run: {len(urls)}")
    print(f"New since last run:  {len(new_urls)}")
    print(f"Saved: {txt_path}")
    print(f"Saved: {json_path}")
    print(f"Seen DB: {seen_path}")


if __name__ == "__main__":
    main()
