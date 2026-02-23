import argparse
import json
import os
import re
import time
from urllib.parse import urlparse, urljoin

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

ATALON_BASE = "https://atalon.hu"

# Tipikus epizód slug: something-s01-e03 (de néha s1-e3 is lehet)
EPISODE_SLUG_RE = re.compile(r"/([a-z0-9-]+-s\d+-e\d+)\b", re.IGNORECASE)

# Dátum a kártyán: 2021. 11. 17.
HU_DATE_RE = re.compile(r"\b(\d{4})\.\s*(\d{2})\.\s*(\d{2})\.?\b")

# Időtartam: 34:52 vagy 01:04:48
DUR_RE = re.compile(r"\b(\d{1,2}:\d{2})(:\d{2})?\b")

AUDIO_URL_RE = re.compile(
    r'https://[^\s"<>]*?\.(?:mp3|m4a|wav|aac)[^\s"<>]*',
    re.IGNORECASE
)


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


# -------------------- requests: extract & download (episode page -> audio url) --------------------

def extract_audio_url_from_html(html: str) -> str | None:
    # __NEXT_DATA__
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

    # fallback
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
    if r.status_code == 404:
        print("  - 404 (nincs ilyen epizód URL) -> kihagyom")
        return False
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


# -------------------- playwright helpers --------------------

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
            page.wait_for_timeout(700)
            return True

        for fr in page.frames:
            if fr == page.main_frame:
                continue
            if try_click(fr):
                page.wait_for_timeout(700)
                return True

        page.wait_for_timeout(400)

    return False


def click_mutass_tobbet(page) -> bool:
    selectors = [
        'text=/^MUTASS\\s+TÖBBET$/i',
        'text=/^Mutass\\s+többet$/i',
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                loc.scroll_into_view_if_needed()
                loc.click(timeout=1200, force=True)
                return True
        except Exception:
            pass
    return False


def scroll_step(page) -> bool:
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


def estimate_episode_count(page) -> int:
    # Tüzetes ellenőrzés: teljes oldal szövegében hány dátum szerepel
    try:
        txt = page.evaluate("() => document.body ? (document.body.innerText || '') : ''")
        if not txt:
            return 0
        return len(HU_DATE_RE.findall(txt))
    except Exception:
        return 0


def detect_creator_page(page) -> bool:
    """
    Creator oldal jelleg:
    - sokszor nincs tömeges dátum/időtartam lista
    - és a fő tartalomban van PODCASTOK szekciócím
    """
    try:
        # "PODCASTOK" cím NEM a menüben: kerüljük nav/aside elemeket
        has_podcastok_heading = page.evaluate("""
        () => {
          const els = Array.from(document.querySelectorAll('h1,h2,h3,div,span'))
            .filter(el => el && el.textContent && el.textContent.trim() === 'PODCASTOK');
          return els.some(el => !el.closest('nav') && !el.closest('aside'));
        }
        """)
    except Exception:
        has_podcastok_heading = False

    # ha nincs legalább 3 dátum a lapon, nagyon gyanús, hogy nem epizódlista
    ep_count_est = estimate_episode_count(page)
    return bool(has_podcastok_heading) and ep_count_est < 3


def extract_visible_episode_cards_dom(page) -> list[dict]:
    """
    DOM-ból: olyan "kártyákat" keresünk, amiben van dátum és valamilyen atalon.hu link.
    Nem ragaszkodunk szigorúan a -s-e mintához, de ha van, előny.
    """
    js = r"""
    () => {
      const reDate = /\b(\d{4})\.\s*(\d{2})\.\s*(\d{2})\.?\b/;
      const reDur = /\b(\d{1,2}:\d{2})(:\d{2})?\b/;

      const out = [];
      const seen = new Set();

      // gyűjtsünk minden elemet, amelynek a szövegében van dátum (ez tipikusan az epizód sor)
      const all = Array.from(document.querySelectorAll('body *'));
      for (const el of all) {
        const txt = (el.innerText || '').trim();
        if (!txt) continue;
        if (!reDate.test(txt)) continue;

        // menjünk fel pár szintet, hogy megtaláljuk a "kártyát"
        let card = el;
        for (let i=0; i<8 && card; i++) {
          // legyen benne időtartam is (epizódokra jellemző)
          const cTxt = (card.innerText || '');
          if (reDate.test(cTxt) && reDur.test(cTxt)) break;
          card = card.parentElement;
        }
        if (!card) continue;

        // kártyán belül keressünk linket
        const links = Array.from(card.querySelectorAll('a[href]'))
          .map(a => a.href)
          .filter(h => typeof h === 'string' && h.startsWith('http'));

        if (links.length === 0) continue;

        // title: próbáljuk a kártya elejéről (első sor)
        let title = '';
        const lines = (card.innerText || '').split('\n').map(s => s.trim()).filter(Boolean);
        if (lines.length > 0) title = lines[0];

        // date: első találat
        const m = (card.innerText || '').match(reDate);
        const date = m ? `${m[1]}.${m[2]}.${m[3]}.` : '';

        // válasszunk egy "legjobb" linket:
        // - preferáljuk azt, ami atalon.hu és NEM image/assets
        let best = '';
        for (const h of links) {
          if (!h.includes('atalon.hu')) continue;
          if (h.includes('/_next/') || h.includes('/assets') || h.includes('.png') || h.includes('.webp')) continue;
          best = h;
          break;
        }
        if (!best) best = links[0];

        if (!seen.has(best)) {
          seen.add(best);
          out.push({url: best, title, date});
        }
      }

      return out;
    }
    """
    try:
        return page.evaluate(js)
    except Exception:
        return []


def scrape_episode_urls_from_json_text(text: str) -> set[str]:
    out = set()
    if not text:
        return out

    # abs URL
    for m in re.finditer(r"https?://atalon\.hu/[a-z0-9/_\-]+", text, flags=re.IGNORECASE):
        u = m.group(0)
        if "/_next/" in u or "/assets" in u:
            continue
        out.add(u)

    # rel slug -sXX-eYY
    for m in EPISODE_SLUG_RE.finditer(text):
        slug = m.group(1)
        out.add(urljoin(ATALON_BASE, "/" + slug))

    return out


def fill_gaps_episode_urls(urls: set[str]) -> set[str]:
    """
    Ha van sok ...-s01-eXX URL, akkor kitaláljuk a hiányzó e számokat.
    Példa: megvan e01,e02,e04,e05 -> legeneráljuk e03-at is.
    """
    # group by (prefix up to -s..., seasonDigitsLen, epDigitsLen)
    groups = {}

    for u in urls:
        p = urlparse(u).path.strip("/")
        m = re.match(r"^(.*)-s(\d+)-e(\d+)$", p, flags=re.IGNORECASE)
        if not m:
            continue
        prefix, s_str, e_str = m.group(1), m.group(2), m.group(3)
        key = (prefix.lower(), s_str)  # season string fixed
        groups.setdefault(key, {"e": set(), "e_len": 0})
        groups[key]["e"].add(int(e_str))
        groups[key]["e_len"] = max(groups[key]["e_len"], len(e_str))

    new_urls = set(urls)
    for (prefix, s_str), info in groups.items():
        if len(info["e"]) < 3:
            continue
        e_min, e_max = min(info["e"]), max(info["e"])
        e_len = info["e_len"] or 1

        for e in range(e_min, e_max + 1):
            if e not in info["e"]:
                ep = str(e).zfill(e_len)
                slug = f"{prefix}-s{s_str}-e{ep}"
                new_urls.add(urljoin(ATALON_BASE, "/" + slug))

    return new_urls


def collect_episodes_hardcore(show_url: str, headful: bool) -> tuple[str, list[dict]]:
    """
    Visszaadja: (ptype, episodes)
    ptype: 'creator' | 'podcast_list'
    episodes: [{url,title,date},...]
    """
    network_urls: set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headful)
        context = browser.new_context()
        page = context.new_page()

        # 1) Response listener: JSON/XHR válaszokból epizód URL-ek kigyűjtése
        def on_response(resp):
            try:
                ct = (resp.headers.get("content-type") or "").lower()
                u = resp.url.lower()
                if ("application/json" in ct) or u.endswith(".json") or ("xhr" in ct) or ("increment.php" in u):
                    txt = resp.text()
                    network_urls.update(scrape_episode_urls_from_json_text(txt))
            except Exception:
                pass

        page.on("response", on_response)

        page.goto(show_url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(1200)
        handle_consent(page, timeout_ms=15000)

        # 2) creator skip: ha gyanús creator oldal
        if detect_creator_page(page):
            page.close()
            browser.close()
            return "creator", []

        # 3) Tüzetes betöltés: mutass többet + scroll + DOM gyűjtés közben
        collected_dom: dict[str, dict] = {}

        expected = max(0, estimate_episode_count(page))
        stable = 0
        last_total = -1

        for _ in range(220):
            handle_consent(page, timeout_ms=2000)

            # mutass többet többször
            for __ in range(4):
                if not click_mutass_tobbet(page):
                    break
                page.wait_for_timeout(700)

            # DOM kártyák
            batch = extract_visible_episode_cards_dom(page)
            for it in batch:
                u = (it.get("url") or "").strip()
                if not u:
                    continue
                if not u.startswith("http"):
                    continue
                # csak atalon.hu internal
                if urlparse(u).netloc and "atalon.hu" not in urlparse(u).netloc:
                    continue
                if "/_next/" in u or "/assets" in u:
                    continue
                if u not in collected_dom:
                    collected_dom[u] = {
                        "url": u,
                        "title": (it.get("title") or "").strip() or None,
                        "date": (it.get("date") or "").strip() or None
                    }
                else:
                    if not collected_dom[u].get("title") and (it.get("title") or "").strip():
                        collected_dom[u]["title"] = (it.get("title") or "").strip()
                    if not collected_dom[u].get("date") and (it.get("date") or "").strip():
                        collected_dom[u]["date"] = (it.get("date") or "").strip()

            # total = DOM + network (de network-et csak URL set-ben tartjuk)
            total = len(collected_dom) + len(network_urls)
            if total == last_total:
                stable += 1
            else:
                stable = 0
                last_total = total

            # ha elértük a becsült epizód számot, és már stabil, megállhatunk
            if expected > 0 and total >= expected and stable >= 6:
                break

            moved = scroll_step(page)
            page.wait_for_timeout(650)

            if stable >= 12 and not moved:
                break

            # néha frissítsük az expected-et, mert később töltődik be a szöveg is
            if _ % 20 == 0:
                expected = max(expected, estimate_episode_count(page))

        # 4) FAILSAFE PASS 2 (felülről újra, ha gyanúsan kevés)
        expected_final = max(expected, estimate_episode_count(page))
        total_first = len(collected_dom) + len(network_urls)

        if expected_final > 0 and total_first < expected_final:
            scroll_to_top_everywhere(page)
            handle_consent(page, timeout_ms=3000)
            stable = 0
            last_total = -1

            for _ in range(180):
                for __ in range(4):
                    if not click_mutass_tobbet(page):
                        break
                    page.wait_for_timeout(650)

                batch = extract_visible_episode_cards_dom(page)
                for it in batch:
                    u = (it.get("url") or "").strip()
                    if not u or "/_next/" in u or "/assets" in u:
                        continue
                    if u not in collected_dom:
                        collected_dom[u] = {
                            "url": u,
                            "title": (it.get("title") or "").strip() or None,
                            "date": (it.get("date") or "").strip() or None
                        }

                total = len(collected_dom) + len(network_urls)
                if total == last_total:
                    stable += 1
                else:
                    stable = 0
                    last_total = total

                if total >= expected_final and stable >= 6:
                    break

                moved = scroll_step(page)
                page.wait_for_timeout(650)

                if stable >= 12 and not moved:
                    break

        # 5) Merge: DOM + network, majd gap-fill (s01-eXX hiányzókat pótoljuk)
        merged_urls = set(collected_dom.keys()) | set(network_urls)
        merged_urls = fill_gaps_episode_urls(merged_urls)

        # meta lista
        episodes = []
        for u in sorted(merged_urls):
            meta = collected_dom.get(u, {"url": u, "title": None, "date": None})
            episodes.append(meta)

        page.close()
        browser.close()

        # ha nincs dátum tömeg és nincs epizód url, akkor creator
        if len(episodes) == 0:
            return "creator", []

        return "podcast_list", episodes


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
                # ha valaki creator oldalt ad meg, ezt most biztosan skippeljük a hardcore collectorral
                ptype, episodes = collect_episodes_hardcore(seed, headful=args.headful)

                if ptype == "creator":
                    print("  - PODCASTOK (creator) oldal -> SKIP (nem jelölöm késznek)")
                    ok_all = False
                else:
                    print(f"  - Talált epizódok (DOM+Network+GapFill): {len(episodes)}")
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