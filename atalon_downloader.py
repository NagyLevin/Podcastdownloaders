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

EPISODE_PATH_RE = re.compile(r"-s\d+-e\d+", re.IGNORECASE)
AUDIO_URL_RE = re.compile(r'https://[^\s"<>]*?\.(?:mp3|m4a|wav|aac)[^\s"<>]*', re.IGNORECASE)
HU_DATE_RE = re.compile(r"\b(\d{4})\.\s*(\d{2})\.\s*(\d{2})\.?\b")


# -------------------- utils --------------------

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
    base = f"{sanitize_filename(date_norm)}__{sanitize_filename(source_slug)}__{sanitize_filename(episode_slug)}__{sanitize_filename(title)}"
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


# -------------------- playwright: NEW approach --------------------

def get_main_area(page):
    # Ha van <main>, azt használjuk; különben a body-t.
    try:
        m = page.locator("main").first
        if m.count() > 0:
            return m
    except Exception:
        pass
    return page.locator("body").first


def is_visible_exact_text(scope, text_exact: str) -> bool:
    # Csak látható elemeket figyelünk, és EXAKT feliratot.
    try:
        loc = scope.locator(f"xpath=.//*[normalize-space()='{text_exact}']").first
        return loc.count() > 0 and loc.is_visible()
    except Exception:
        return False


def handle_consent(page, timeout_ms: int = 15000) -> bool:
    end = time.time() + timeout_ms / 1000.0

    accept_selectors = [
        'button:has-text("ELFOGADOM")',
        '[role="button"]:has-text("ELFOGADOM")',
        'text=ELFOGADOM',
        'button:has-text("Elfogadom")',
        '[role="button"]:has-text("Elfogadom")',
        'text=Elfogadom',
        'text=/^ACCEPT$/i',
        'text=/^Accept$/i',
    ]

    def try_click(ctx) -> bool:
        for sel in accept_selectors:
            try:
                loc = ctx.locator(sel).first
                if loc.count() > 0 and loc.is_visible():
                    loc.click(timeout=1200, force=True)
                    return True
            except Exception:
                pass
        return False

    while time.time() < end:
        if try_click(page):
            page.wait_for_timeout(800)
            return True
        for fr in page.frames:
            if fr == page.main_frame:
                continue
            if try_click(fr):
                page.wait_for_timeout(800)
                return True
        page.wait_for_timeout(500)

    return False


def click_mutass_tobbet(scope) -> bool:
    # Text regexes selector: ha bármi ilyen felirat van, kattint.
    candidates = [
        'text=/^MUTASS\\s+TÖBBET$/i',
        'text=/^Mutass\\s+többet$/i',
    ]
    for sel in candidates:
        try:
            loc = scope.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                loc.scroll_into_view_if_needed()
                loc.click(timeout=1500, force=True)
                return True
        except Exception:
            pass
    return False


def scroll_step(page) -> bool:
    # Sokszor nem a window scrolloz, hanem egy belső konténer.
    try:
        moved = page.evaluate("""
        () => {
          let moved = false;

          const y0 = window.scrollY;
          window.scrollBy(0, window.innerHeight * 0.9);
          if (window.scrollY !== y0) moved = true;

          const scrollers = Array.from(document.querySelectorAll('*')).filter(el => {
            const s = getComputedStyle(el);
            const oy = s.overflowY;
            return (oy === 'auto' || oy === 'scroll') && el.scrollHeight > el.clientHeight + 50;
          });

          scrollers.sort((a,b) => (b.scrollHeight-b.clientHeight) - (a.scrollHeight-a.clientHeight));

          if (scrollers.length > 0) {
            const el = scrollers[0];
            const before = el.scrollTop;
            el.scrollTop = Math.min(el.scrollTop + el.clientHeight * 0.9, el.scrollHeight);
            if (el.scrollTop !== before) moved = true;
          }

          return moved;
        }
        """)
        return bool(moved)
    except Exception:
        return False


def scroll_to_top_everywhere(page) -> None:
    try:
        page.evaluate("""
        () => {
          window.scrollTo(0, 0);
          const scrollers = Array.from(document.querySelectorAll('*')).filter(el => {
            const s = getComputedStyle(el);
            const oy = s.overflowY;
            return (oy === 'auto' || oy === 'scroll') && el.scrollHeight > el.clientHeight + 50;
          });
          for (const el of scrollers) el.scrollTop = 0;
        }
        """)
    except Exception:
        pass
    page.wait_for_timeout(800)


def extract_visible_episode_meta(page) -> list[dict]:
    """
    LÁTHATÓ/DOM-ban lévő epizódok meta kinyerése (url/title/date).
    Ezt sokszor hívjuk scroll közben -> nem marad ki a virtualizált lista miatt.
    """
    js = r"""
    () => {
      const reEp = /-s\d+-e\d+/i;
      const reDate = /(\d{4})\.\s*(\d{2})\.\s*(\d{2})\.?/;

      const out = [];
      const seen = new Set();

      const links = Array.from(document.querySelectorAll('a[href]'))
        .filter(a => reEp.test(a.href || ''));

      for (const a of links) {
        const href = a.href || '';
        if (!href || seen.has(href)) continue;
        seen.add(href);

        // Keressük a legkisebb "kártya" konténert: dátum benne van + kevés epizód link
        let node = a;
        let best = null;

        for (let i = 0; i < 25 && node; i++) {
          const txt = (node.innerText || '');
          const hasDate = reDate.test(txt);

          if (hasDate) {
            const linksHere = Array.from(node.querySelectorAll('a[href]'))
              .filter(x => reEp.test(x.href || ''))
              .length;

            // ha kicsi a konténer (1-2 epizód link), nagy eséllyel ez az epizód sora
            if (linksHere <= 2) { best = node; break; }

            // fallback: első dátumos ős
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
    try:
        return page.evaluate(js)
    except Exception:
        return []


def detect_creator_vs_podcastlist(page) -> str:
    """
    Csak a fő tartalomrészben vizsgálunk EXAKT feliratot, és csak láthatót.
    - creator: PODCASTOK látható, EPIZÓDOK nem látható
    - podcast_list: EPIZÓDOK látható
    - unknown: más
    """
    main = get_main_area(page)
    has_epizodok = is_visible_exact_text(main, "EPIZÓDOK")
    has_podcastok = is_visible_exact_text(main, "PODCASTOK")

    if has_epizodok:
        return "podcast_list"
    if has_podcastok and not has_epizodok:
        return "creator"
    return "unknown"


def collect_episodes_from_show_page(show_url: str, headful: bool) -> tuple[str, list[dict]]:
    """
    ÚJ stratégia:
    - consent kezelése
    - creator/podcast_list detektálás a MAIN tartalomból
    - scroll + 'Mutass többet' közben folyamatos gyűjtés
    - failsafe: 2. teljes passz (felülről újra)
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headful)
        context = browser.new_context()
        page = context.new_page()

        page.goto(show_url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(1200)
        handle_consent(page, timeout_ms=15000)

        ptype = detect_creator_vs_podcastlist(page)
        if ptype == "creator":
            # ez az a főoldal, amit kérsz, hogy skippeljük
            page.close()
            browser.close()
            return "creator", []

        main = get_main_area(page)
        base_netloc = urlparse(show_url).netloc

        def run_pass(max_steps: int = 160) -> dict[str, dict]:
            collected: dict[str, dict] = {}
            stable_no_new = 0

            for _ in range(max_steps):
                handle_consent(page, timeout_ms=2000)

                # "Mutass többet" próbálkozás többször, mert néha későn jelenik meg
                for __ in range(3):
                    if not click_mutass_tobbet(main):
                        break
                    page.wait_for_timeout(700)

                batch = extract_visible_episode_meta(page)
                new_cnt = 0

                for it in batch:
                    href = (it.get("href") or "").strip()
                    if not href:
                        continue
                    pu = urlparse(href)
                    if pu.scheme not in ("http", "https"):
                        continue
                    if pu.netloc != base_netloc:
                        continue
                    if not EPISODE_PATH_RE.search(pu.path or ""):
                        continue

                    if href not in collected:
                        collected[href] = {
                            "url": href,
                            "title": (it.get("title") or "").strip() or None,
                            "date": (it.get("date") or "").strip() or None
                        }
                        new_cnt += 1
                    else:
                        if not collected[href].get("title") and (it.get("title") or "").strip():
                            collected[href]["title"] = (it.get("title") or "").strip()
                        if not collected[href].get("date") and (it.get("date") or "").strip():
                            collected[href]["date"] = (it.get("date") or "").strip()

                if new_cnt == 0:
                    stable_no_new += 1
                else:
                    stable_no_new = 0

                moved = scroll_step(page)
                page.wait_for_timeout(650)

                # ha sokáig nincs új ÉS már scroll se mozdul, akkor vége
                if stable_no_new >= 10 and not moved:
                    break

            return collected

        # PASS 1
        c1 = run_pass()
        print(f"  - PASS 1: {len(c1)} epizód")

        # FAILSAFE PASS 2 (felülről újra)
        scroll_to_top_everywhere(page)
        handle_consent(page, timeout_ms=3000)
        c2 = run_pass()
        print(f"  - PASS 2: {len(c2)} epizód")

        merged = dict(c1)
        for k, v in c2.items():
            if k not in merged:
                merged[k] = v
            else:
                if not merged[k].get("title") and v.get("title"):
                    merged[k]["title"] = v["title"]
                if not merged[k].get("date") and v.get("date"):
                    merged[k]["date"] = v["date"]

        episodes = list(merged.values())
        episodes.sort(key=lambda x: x["url"])

        # Ha még mindig 0 epizód, akkor gyakorlatban creator/invalid -> jelöljük unknown-nak, és a hívó oldalon kezeljük
        if len(episodes) == 0 and ptype == "unknown":
            # utolsó check: van-e egyáltalán epizód link a DOM-ban
            dom_hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
            has_any_episode = any(EPISODE_PATH_RE.search(urlparse(h).path or "") for h in dom_hrefs if isinstance(h, str))
            if not has_any_episode:
                ptype = "creator"

        page.close()
        browser.close()
        return ptype, episodes


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

    seeds = parse_input_urls(read_input_lines(args.input))
    if not seeds:
        print("Nincs feldolgozható URL az inputban (lehet, hogy mind #).")
        return

    done_seeds = set()

    with requests.Session() as session:
        session.headers.update(DEFAULT_HEADERS)

        for seed in seeds:
            print(f"\n=== Seed: {seed} ===")
            ok_all = True

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
                    ptype, episodes = collect_episodes_from_show_page(seed, headful=args.headful)

                    if ptype == "creator":
                        print("  - Creator/Podcastok főoldal -> SKIP (nem jelölöm késznek)")
                        ok_all = False
                    else:
                        print(f"  - Talált epizódok összesen: {len(episodes)}")
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
                print("  - Seed NEM teljesen sikeres / SKIP, nem jelölöm késznek.")

    if done_seeds:
        mark_done_in_input_file(args.input, done_seeds)

    print("\nMinden kész.")


if __name__ == "__main__":
    main()