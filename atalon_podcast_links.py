from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from urllib.parse import urlparse, urljoin
import re

START_URL = "https://atalon.hu/podcastok"
OUTFILE = "atalon_podcast_links.txt"

# "all" | "shows" | "episodes"
MODE = "all"

MAX_SCROLLS = 250
NO_NEW_LIMIT = 6
SCROLL_WAIT_MS = 1200

BASE = "https://atalon.hu"
EPISODE_SUFFIX_RE = re.compile(r"-[a-z0-9]{6}$", re.I)

EXCLUDE_PATHS = {
    "/", "/podcastok", "/idegen-nyelvu-podcastok", "/radiok", "/kategoriak",
    "/podcastereknek", "/adatkezelesi-nyilatkozat", "/jogi-nyilatkozat", "/impresszum"
}

def normalize(href: str) -> str:
    href = href.strip()
    if href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
        return ""
    # relatív -> abszolút
    abs_url = urljoin(BASE, href)
    # query + fragment le
    p = urlparse(abs_url)
    return f"{p.scheme}://{p.netloc}{p.path}".rstrip("/")

def is_episode(path: str) -> bool:
    # /valami-qyltth jellegű (6 karakteres) végződés
    slug = path.strip("/").split("/")[-1]
    return bool(EPISODE_SUFFIX_RE.search(slug))

def keep(url: str) -> bool:
    if not url:
        return False
    p = urlparse(url)

    if p.scheme not in ("http", "https"):
        return False
    if p.netloc not in ("atalon.hu", "www.atalon.hu"):
        return False

    path = p.path.rstrip("/")
    if not path:
        path = "/"

    # kizárások (menü, jogi, stb.)
    if path in EXCLUDE_PATHS:
        return False

    # tipikus kategória oldalak: /valami/podcastok (pl. /gazdasag/podcastok)
    # ha csak "műsor" linkeket akarsz, ezeket általában érdemes kihagyni
    if path.endswith("/podcastok") and path != "/podcastok":
        return False

    if MODE == "shows" and is_episode(path):
        return False
    if MODE == "episodes" and not is_episode(path):
        return False

    return True

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)  # ha gond van: headless=False
    page = browser.new_page(viewport={"width": 1400, "height": 900})

    page.goto(START_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(1500)

    # cookie/consent próbálkozás (ha van)
    for label in ["Elfogadom", "Accept", "I agree", "Rendben", "OK", "Got it"]:
        try:
            page.get_by_role("button", name=label).click(timeout=1200)
            break
        except Exception:
            pass

    seen = set()
    last_count = 0
    no_new = 0

    for _ in range(MAX_SCROLLS):
        hrefs = page.eval_on_selector_all(
            "a[href]",
            "els => els.map(e => e.getAttribute('href'))"
        )

        for h in hrefs:
            url = normalize(h or "")
            if keep(url):
                seen.add(url)

        if len(seen) == last_count:
            no_new += 1
        else:
            last_count = len(seen)
            no_new = 0

        if no_new >= NO_NEW_LIMIT:
            break

        # görgetés
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(SCROLL_WAIT_MS)

        # ha van "Tovább / Load more" gomb, próbáld megnyomni (nem mindig létezik)
        try:
            page.get_by_role("button", name=re.compile(r"(tov[áa]bb|load more|mutass t[öo]bbet)", re.I)).click(timeout=800)
            page.wait_for_timeout(800)
        except Exception:
            pass

    browser.close()

with open(OUTFILE, "w", encoding="utf-8") as f:
    for url in sorted(seen):
        f.write(url + "\n")

print(f"Mentve: {OUTFILE} | linkek száma: {len(seen)} | MODE={MODE}")