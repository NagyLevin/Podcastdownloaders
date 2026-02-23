# soundcloud_playlists_scrape.py
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import re

QUERY = "magyar"
START_URL = f"https://soundcloud.com/search?q={QUERY}"
FALLBACK_SETS_URL = f"https://soundcloud.com/search/sets?q={QUERY}"
OUTFILE = f"soundcloud_playlists_{QUERY}.txt"

MAX_SCROLLS = 200          # biztonsági limit
NO_NEW_LIMIT = 5           # ennyi "nem nőtt az új linkek száma" után megáll

def normalize(href: str) -> str:
    href = href.split("?")[0]
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return "https://soundcloud.com" + href
    return href

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)  # ha látni akarod: headless=False
    page = browser.new_page(viewport={"width": 1400, "height": 900})

    page.goto(START_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(1500)

    # Cookie/consent felugrók (ha vannak)
    try:
        page.get_by_role("button", name=re.compile(r"accept|agree|ok|got it", re.I)).click(timeout=2500)
    except Exception:
        pass

    # Kattints a Playlists (sets) fülre; ha nem megy, nyisd meg direktben
    try:
        page.locator('a[href^="/search/sets"]').first.click(timeout=5000)
    except PlaywrightTimeoutError:
        page.goto(FALLBACK_SETS_URL, wait_until="domcontentloaded")

    page.wait_for_timeout(2000)

    seen = set()
    last_count = 0
    no_new = 0

    for _ in range(MAX_SCROLLS):
        hrefs = page.eval_on_selector_all(
            "a[href]",
            "els => els.map(e => e.getAttribute('href'))"
        )

        for h in hrefs:
            if not h:
                continue
            if "/sets/" in h:
                seen.add(normalize(h))

        if len(seen) == last_count:
            no_new += 1
        else:
            last_count = len(seen)
            no_new = 0

        if no_new >= NO_NEW_LIMIT:
            break

        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1200)

    browser.close()

with open(OUTFILE, "w", encoding="utf-8") as f:
    for url in sorted(seen):
        f.write(url + "\n")

print(f"Mentve: {OUTFILE} | playlist URL-ek: {len(seen)}")