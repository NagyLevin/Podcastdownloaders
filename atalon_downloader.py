import argparse
import json
import os
import re
import time
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

# Epizód URL minta: ...-s01-e14
EPISODE_PATH_RE = re.compile(r"-s\d+-e\d+", re.IGNORECASE)

# MP3/M4A/WAV/AAC direkt link keresés (__NEXT_DATA__ / HTML-ben)
AUDIO_URL_RE = re.compile(
    r'https://[^\s"<>]*?\.(?:mp3|m4a|wav|aac)[^\s"<>]*',
    re.IGNORECASE
)

# Dátum: 2022. 02. 02. (záró pont opcionális)
HU_DATE_RE = re.compile(r"\b(\d{4})\.\s*(\d{2})\.\s*(\d{2})\.?\b")


# -------------------- basic utils --------------------

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def sanitize_filename(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r"[\\/*?\"<>|:]", "", name).strip()
    name = re.sub(r"\s+", " ", name)
    return name or "podcast_adas"


def truncate(s: str, max_len: int = 180) -> str:
    s = (s or "").strip()
    return s if len(s) <= max_len else s[:max_len].rstrip()


def is_episode_url(u: str) -> bool:
    p = urlparse(u)
    return bool(EPISODE_PATH_RE.search(p.path or ""))


def slug_from_url(u: str) -> str:
    path = (urlparse(u).path or "").strip("/")
    if not path:
        return "root"
    return sanitize_filename(path.split("/")[-1])


def normalize_date(date_str: str | None) -> str:
    if not date_str:
        return "unknown-date"
    m = HU_DATE_RE.search(date_str)
    if m:
        y, mo, d = m.group(1), m.group(2), m.group(3)
        return f"{y}-{mo}-{d}"
    if re.match(r"^\d{4}-\d{2}-\d{2}", date_str):
        return date_str[:10]
    return "unknown-date"


# -------------------- visited --------------------

def load_visited(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_visited(path: str, data: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# -------------------- input file (# marking) --------------------

def read_input_lines(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        return f.readlines()


def parse_input_urls(lines: list[str]) -> list[str]:
    urls = []
    seen = set()
    for ln in lines:
        s = ln.strip()
        if not s or s.startswith("#"):
            continue
        if s not in seen:
            seen.add(s)
            urls.append(s)
    return urls


def mark_done_in_input_file(input_path: str, done_urls: set[str]) -> None:
    lines = read_input_lines(input_path)
    out_lines = []

    for ln in lines:
        raw = ln.rstrip("\n")
        stripped = raw.strip()

        if not stripped or stripped.startswith("#"):
            out_lines.append(ln)
            continue

        if stripped in done_urls:
            out_lines.append("# " + stripped + "\n")
        else:
            out_lines.append(ln if ln.endswith("\n") else ln + "\n")

    with open(input_path, "w", encoding="utf-8") as f:
        f.writelines(out_lines)


# -------------------- requests: extract & download --------------------

def extract_audio_url_from_html(html: str) -> str | None:
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

    url_match = AUDIO_URL_RE.search(html)
    if url_match:
        return url_match.group(0).replace("\\u0026", "&")

    return None


def guess_title_from_html(html: str) -> str | None:
    m = re.search(r"<title>\s*(.*?)\s*</title>", html, re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    t = re.sub(r"\s+", " ", m.group(1)).strip()
    t = re.sub(r"\s*\|\s*Atalon\s*$", "", t, flags=re.IGNORECASE).strip()
    return t or None


def guess_date_from_html(html: str) -> str | None:
    m = HU_DATE_RE.search(html)
    if not m:
        return None
    return f"{m.group(1)}.{m.group(2)}.{m.group(3)}."


def extension_from_audio_url(audio_url: str) -> str:
    lower = audio_url.lower()
    for ext in (".mp3", ".m4a", ".wav", ".aac"):
        if ext in lower:
            return ext
    return ".mp3"


def build_output_filename(date_norm: str, source_slug: str, episode_slug: str, title: str, ext: str) -> str:
    source_slug = sanitize_filename(source_slug)
    episode_slug = sanitize_filename(episode_slug)
    title = sanitize_filename(title)

    base = f"{date_norm}__{source_slug}__{episode_slug}__{title}"
    base = truncate(base, 180)
    return base + ext


def download_audio(session: requests.Session, audio_url: str, out_path: str, referer: str) -> None:
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        print(f"  - Már létezik, kihagyom: {out_path}")
        return

    headers = dict(DEFAULT_HEADERS)
    headers["Referer"] = referer

    with session.get(audio_url, stream=True, headers=headers, timeout=180) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def download_episode_requests(
    episode_url: str,
    out_dir: str,
    session: requests.Session,
    visited: dict,
    source_slug: str,
    known_title: str | None = None,
    known_date: str | None = None
) -> bool:
    if episode_url in visited:
        print(f"Epizód kihagyva (visited): {episode_url}")
        return True

    print(f"Epizód elemzése: {episode_url}")
    r = session.get(episode_url, headers=DEFAULT_HEADERS, timeout=45)
    r.raise_for_status()
    html = r.text

    audio_url = extract_audio_url_from_html(html)
    if not audio_url:
        print("  - Nem találtam audio URL-t (__NEXT_DATA__/HTML).")
        return False

    title = known_title or guess_title_from_html(html) or slug_from_url(episode_url)
    date_raw = known_date or guess_date_from_html(html)
    date_norm = normalize_date(date_raw)

    ep_slug = slug_from_url(episode_url)
    ext = extension_from_audio_url(audio_url)

    filename = build_output_filename(date_norm, source_slug, ep_slug, title, ext)
    out_path = os.path.join(out_dir, filename)

    print(f"  - Audio: {audio_url}")
    print(f"  - Fájl: {out_path}")
    download_audio(session, audio_url, out_path, referer=episode_url)

    visited[episode_url] = {
        "title": title,
        "date": date_norm,
        "source": source_slug,
        "folder": os.path.basename(out_dir),
        "filename": filename
    }
    print("  - KÉSZ")
    return True


# -------------------- playwright: consent + load more + stable collect --------------------

def handle_consent(page, timeout_ms: int = 15000) -> bool:
    """
    Robusztus consent popup kattintás:
    - vár és újrapróbálkozik
    - main frame + iframes
    - többféle selector
    - force click
    """
    end = time.time() + timeout_ms / 1000.0

    accept_selectors = [
        'button:has-text("ELFOGADOM")',
        '[role="button"]:has-text("ELFOGADOM")',
        'text=ELFOGADOM',
        'button:has-text("Elfogadom")',
        '[role="button"]:has-text("Elfogadom")',
        'text=Elfogadom',
        'button:has-text("ACCEPT")',
        '[role="button"]:has-text("ACCEPT")',
        'text=ACCEPT',
        'button:has-text("Accept")',
        '[role="button"]:has-text("Accept")',
        'text=Accept',
    ]

    reject_selectors = [
        'button:has-text("NEM FOGADOM EL")',
        '[role="button"]:has-text("NEM FOGADOM EL")',
        'text=NEM FOGADOM EL',
        'button:has-text("Nem fogadom el")',
        '[role="button"]:has-text("Nem fogadom el")',
        'text=Nem fogadom el',
        'button:has-text("REJECT")',
        '[role="button"]:has-text("REJECT")',
        'text=REJECT',
        'button:has-text("Reject")',
        '[role="button"]:has-text("Reject")',
        'text=Reject',
    ]

    def try_click(ctx, selectors) -> bool:
        for sel in selectors:
            try:
                loc = ctx.locator(sel).first
                if loc.count() > 0:
                    loc.click(timeout=800, force=True)
                    return True
            except Exception:
                pass
        return False

    while time.time() < end:
        if try_click(page, accept_selectors):
            page.wait_for_timeout(800)
            try:
                page.wait_for_load_state("domcontentloaded", timeout=5000)
            except Exception:
                pass
            return True

        for fr in page.frames:
            if fr == page.main_frame:
                continue
            if try_click(fr, accept_selectors):
                page.wait_for_timeout(800)
                try:
                    page.wait_for_load_state("domcontentloaded", timeout=5000)
                except Exception:
                    pass
                return True

        # fallback reject (legalább eltűnjön)
        if try_click(page, reject_selectors):
            page.wait_for_timeout(800)
            return True

        for fr in page.frames:
            if fr == page.main_frame:
                continue
            if try_click(fr, reject_selectors):
                page.wait_for_timeout(800)
                return True

        page.wait_for_timeout(500)

    return False


def click_show_more_if_present(page, max_clicks: int = 50) -> int:
    """
    Addig kattint a 'MUTASS TÖBBET' gombra (ha van), amíg eltűnik / már nem kattintható.
    Többféle selectorral próbálja.
    """
    selectors = [
        'text=MUTASS TÖBBET',
        'text=Mutass többet',
        'button:has-text("MUTASS TÖBBET")',
        '[role="button"]:has-text("MUTASS TÖBBET")',
        'button:has-text("Mutass többet")',
        '[role="button"]:has-text("Mutass többet")',
    ]

    clicks = 0
    for _ in range(max_clicks):
        clicked = False
        for sel in selectors:
            try:
                loc = page.locator(sel).first
                if loc.count() > 0 and loc.is_visible():
                    loc.click(timeout=1200, force=True)
                    page.wait_for_timeout(700)
                    clicks += 1
                    clicked = True
                    break
            except Exception:
                pass

        if not clicked:
            break

    return clicks


def count_episode_links_on_page(page) -> int:
    try:
        hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
        cnt = 0
        for h in hrefs:
            try:
                if EPISODE_PATH_RE.search(urlparse(h).path or ""):
                    cnt += 1
            except Exception:
                pass
        return cnt
    except Exception:
        return 0


def load_all_episodes_by_scrolling_and_more(page, max_steps: int = 40, pause_ms: int = 800) -> None:
    """
    Kombinált stratégia:
    - consent kezelése
    - scroll + 'Mutass többet' kattintás
    - addig, amíg az epizód linkek száma stabilizálódik
    """
    stable = 0
    last = -1

    for _ in range(max_steps):
        # próbáljuk levadászni a consentet (ha később jelenik meg)
        handle_consent(page, timeout_ms=2500)

        # kattintsunk rá a "Mutass többet"-re, ha van
        click_show_more_if_present(page, max_clicks=3)

        # scroll: window + belső scroller elemek
        page.evaluate("""
        () => {
          window.scrollTo(0, document.body.scrollHeight);
          const scrollers = Array.from(document.querySelectorAll('*')).filter(el => {
            const s = getComputedStyle(el);
            const oy = s.overflowY;
            return (oy === 'auto' || oy === 'scroll') && el.scrollHeight > el.clientHeight;
          });
          for (const el of scrollers) el.scrollTop = el.scrollHeight;
        }
        """)
        page.wait_for_timeout(pause_ms)

        cnt = count_episode_links_on_page(page)

        if cnt == last:
            stable += 1
        else:
            stable = 0
            last = cnt

        # ha 3 kör óta nem nő, elég
        if stable >= 3:
            break


def collect_episode_links_with_meta(show_url: str, headful: bool) -> list[dict]:
    items = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headful)
        context = browser.new_context()
        page = context.new_page()
        page.goto(show_url, wait_until="domcontentloaded", timeout=60000)

        page.wait_for_timeout(1200)
        # consent elsőre + később is lesz még próbálkozás a loopban
        handle_consent(page, timeout_ms=15000)

        # itt töltjük be az összes epizódot: scroll + mutass többet + stabilizálódás
        load_all_episodes_by_scrolling_and_more(page, max_steps=40, pause_ms=800)

        base_netloc = urlparse(show_url).netloc

        # DOM-ból szedjük ki az epizódokat + dátumot "kártya konténerből"
        js = r"""
        () => {
          const reEp = /-s\d+-e\d+/i;
          const reDate = /(\d{4})\.\s*(\d{2})\.\s*(\d{2})\.?/;

          const out = [];
          const seen = new Set();

          const allLinks = Array.from(document.querySelectorAll('a[href]'));
          const epLinks = allLinks.filter(a => reEp.test(a.href || ''));

          for (const a of epLinks) {
            const href = a.href || '';
            if (!href) continue;
            if (seen.has(href)) continue;
            seen.add(href);

            // legkisebb konténer: dátumot tartalmaz és csak 1 ep link van benne
            let node = a;
            let best = null;

            while (node && node !== document.body) {
              const txt = (node.innerText || '');
              const hasDate = reDate.test(txt);

              if (hasDate) {
                const linksHere = Array.from(node.querySelectorAll('a[href]'))
                  .filter(x => reEp.test(x.href || ''))
                  .length;

                if (linksHere === 1) { best = node; break; }
                if (!best) best = node;
              }
              node = node.parentElement;
            }

            let title = (a.textContent || '').trim();
            if (!title && best) {
              const h = best.querySelector('h1,h2,h3,h4');
              if (h) title = (h.textContent || '').trim();
            }

            let date = '';
            if (best) {
              const m = (best.innerText || '').match(reDate);
              if (m) date = `${m[1]}.${m[2]}.${m[3]}.`;
            }

            out.push({href, title, date});
          }

          return out;
        }
        """
        raw = page.evaluate(js)

        seen = set()
        for it in raw:
            href = (it.get("href") or "").strip()
            if not href:
                continue
            pu = urlparse(href)
            if pu.scheme not in ("http", "https"):
                continue
            if pu.netloc != base_netloc:
                continue
            if href in seen:
                continue
            seen.add(href)

            items.append({
                "url": href,
                "title": (it.get("title") or "").strip() or None,
                "date": (it.get("date") or "").strip() or None
            })

        page.close()
        browser.close()

    return items


# -------------------- main --------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Bemeneti txt (soronként 1 link, # = kihagy).")
    ap.add_argument("--out", default="atalon", help="Kimeneti mappa (default: atalon).")
    ap.add_argument("--visited", default="visited.json", help="Visited fájl (default: visited.json).")
    ap.add_argument("--headful", action="store_true", help="Mutassa a böngészőt (debug).")
    args = ap.parse_args()

    ensure_dir(args.out)
    visited = load_visited(args.visited)

    lines = read_input_lines(args.input)
    seeds = parse_input_urls(lines)
    if not seeds:
        print("Nincs feldolgozható URL az inputban (lehet, hogy mind #).")
        return

    done_seeds = set()

    with requests.Session() as session:
        session.headers.update(DEFAULT_HEADERS)

        for seed in seeds:
            print(f"\n=== Seed: {seed} ===")
            ok_all = True

            # seed -> almappa: out/<seed-slug>/
            source_slug = slug_from_url(seed)
            out_dir_seed = os.path.join(args.out, source_slug)
            ensure_dir(out_dir_seed)

            try:
                if is_episode_url(seed):
                    ok = download_episode_requests(
                        episode_url=seed,
                        out_dir=out_dir_seed,
                        session=session,
                        visited=visited,
                        source_slug=source_slug,
                        known_title=None,
                        known_date=None
                    )
                    ok_all = ok_all and ok
                else:
                    episodes = collect_episode_links_with_meta(seed, headful=args.headful)
                    print(f"  - Talált epizódok: {len(episodes)}")

                    if not episodes:
                        ok_all = False
                    else:
                        for ep in episodes:
                            ok = download_episode_requests(
                                episode_url=ep["url"],
                                out_dir=out_dir_seed,
                                session=session,
                                visited=visited,
                                source_slug=source_slug,
                                known_title=ep.get("title"),
                                known_date=ep.get("date")
                            )
                            if not ok:
                                ok_all = False

            except Exception as e:
                print(f"  - Seed hiba: {e}")
                ok_all = False

            save_visited(args.visited, visited)

            if ok_all:
                done_seeds.add(seed)
                print("  - Seed kész, megjelölöm # -tel az inputban.")
            else:
                print("  - Seed NEM teljesen sikeres, nem jelölöm késznek.")

    if done_seeds:
        mark_done_in_input_file(args.input, done_seeds)

    print("\nMinden kész.")


if __name__ == "__main__":
    main()