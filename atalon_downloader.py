import argparse
import json
import os
import random
import re
import time
from urllib.parse import urlparse, urljoin

import requests
from requests.exceptions import ConnectTimeout, ReadTimeout, ConnectionError as ReqConnectionError, HTTPError
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

# /garami-s01-e03
EPISODE_SLUG_RE = re.compile(r"/([a-z0-9-]+-s\d+-e\d+)\b", re.IGNORECASE)

# 2021. 11. 17.
HU_DATE_RE = re.compile(r"\b(\d{4})\.\s*(\d{2})\.\s*(\d{2})\.?\b")

AUDIO_URL_RE = re.compile(
    r'https://[^\s"<>]*?\.(?:mp3|m4a|wav|aac)[^\s"<>]*',
    re.IGNORECASE
)

# Biztonsági sapka a predikcióra
MAX_PREDICT_EP = 500


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


def is_episode_url(u: str) -> bool:
    return bool(EPISODE_SLUG_RE.search(urlparse(u).path or ""))


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


# -------------------- requests: episode page -> audio url -> download --------------------

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
    """
    Retry/backoff csak hálózati hibákra és 5xx-re.
    404 (és általában 4xx, kivéve 429) esetén NINCS retry.
    """
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        print(f"  - Már létezik, kihagyom: {out_path}")
        return

    headers = dict(DEFAULT_HEADERS)
    headers["Referer"] = referer

    timeout = (15, 300)  # (connect, read)
    max_tries = 6
    base_sleep = 2.0

    last_err = None

    for attempt in range(1, max_tries + 1):
        try:
            with session.get(audio_url, stream=True, headers=headers, timeout=timeout) as r:
                # 404 -> azonnali stop (nincs retry)
                if r.status_code == 404:
                    raise HTTPError("404 Client Error: Not Found", response=r)

                r.raise_for_status()

                with open(out_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            return

        except HTTPError as e:
            last_err = e
            code = e.response.status_code if getattr(e, "response", None) is not None else None

            # 404 / 4xx (kivéve 429) -> NINCS retry
            if code is not None and 400 <= code < 500 and code != 429:
                raise

            sleep_s = base_sleep * (2 ** (attempt - 1))
            sleep_s = min(sleep_s, 60.0) + random.uniform(0, 0.8)
            print(f"  - Letöltési hiba (próba {attempt}/{max_tries}): {e}")
            print(f"  - Újrapróbálom {sleep_s:.1f} mp múlva...")
            time.sleep(sleep_s)

        except (ConnectTimeout, ReadTimeout, ReqConnectionError) as e:
            last_err = e
            sleep_s = base_sleep * (2 ** (attempt - 1))
            sleep_s = min(sleep_s, 60.0) + random.uniform(0, 0.8)
            print(f"  - Letöltési hiba (próba {attempt}/{max_tries}): {e}")
            print(f"  - Újrapróbálom {sleep_s:.1f} mp múlva...")
            time.sleep(sleep_s)

    raise RuntimeError(f"Letöltés sikertelen {max_tries} próbálkozás után: {audio_url}\nUtolsó hiba: {last_err}")


def download_episode_requests(
    episode_url: str,
    out_dir: str,
    session: requests.Session,
    visited: dict,
    source_slug: str,
    known_title: str | None = None,
    known_date: str | None = None,
    predicted: bool = False
) -> bool:
    if episode_url in visited:
        print(f"Epizód kihagyva (visited): {episode_url}")
        return True

    print(f"Epizód elemzése: {episode_url}" + (" [PREDICT]" if predicted else ""))
    r = session.get(episode_url, headers=DEFAULT_HEADERS, timeout=45)

    if r.status_code == 404:
        visited[episode_url] = {"missing": True, "reason": "episode_404"}
        print("  - 404 (epizód oldal nincs) -> jelölöm missing-ként")
        return True if predicted else False

    r.raise_for_status()
    html = r.text

    audio_url = extract_audio_url_from_html(html)
    if not audio_url:
        visited[episode_url] = {"missing": True, "reason": "no_audio"}
        print("  - Nincs audio URL -> jelölöm missing-ként")
        return True if predicted else False

    ep_slug = slug_from_url(episode_url)

    title = known_title or guess_title_from_html(html) or ep_slug
    if (title or "").strip().upper() == "EPIZÓDOK":
        title = ep_slug

    date_raw = known_date or guess_date_from_html(html)
    date_norm = normalize_date(date_raw)

    ext = extension_from_audio_url(audio_url)
    filename = build_output_filename(date_norm, source_slug, ep_slug, title, ext)
    out_path = os.path.join(out_dir, filename)

    print(f"  - Audio: {audio_url}")
    print(f"  - Fájl: {out_path}")

    try:
        download_audio(session, audio_url, out_path, referer=episode_url)
    except HTTPError as e:
        code = e.response.status_code if getattr(e, "response", None) is not None else None
        if code == 404:
            # mp3 404 -> predikciónál skip, normálnál fail
            visited[episode_url] = {"missing": True, "reason": "audio_404"}
            print("  - MP3 404 -> jelölöm missing-ként és skipelem")
            return True if predicted else False

        print(f"  - Letöltési HTTP hiba: {e}")
        return False

    except Exception as e:
        print(f"  - Letöltési hiba: {e}")
        return False

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


def select_all_seasons_if_present(page) -> bool:
    try:
        if page.locator("text=/évad/i").count() == 0:
            return False
    except Exception:
        return False

    trigger_selectors = [
        "text=/\\b\\d+\\.\\s*évad\\b/i",
        "text=/Összes\\s+évad/i",
        "button:has-text('évad')",
        "[role='button']:has-text('évad')",
    ]

    trigger = None
    for sel in trigger_selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                trigger = loc
                break
        except Exception:
            pass

    if trigger is None:
        return False

    try:
        trigger.click(timeout=1500, force=True)
        page.wait_for_timeout(500)
    except Exception:
        return False

    clicked = False
    try:
        opts = page.locator("text=/Összes\\s+évad/i")
        n = opts.count()
        for i in range(n):
            el = opts.nth(i)
            if el.is_visible():
                el.scroll_into_view_if_needed()
                el.click(timeout=1500, force=True)
                clicked = True
                break
    except Exception:
        pass

    if clicked:
        page.wait_for_timeout(1200)

    try:
        page.keyboard.press("Escape")
    except Exception:
        pass

    return clicked


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


def estimate_episode_count_by_dates(page) -> int:
    try:
        txt = page.evaluate("() => document.body ? (document.body.innerText || '') : ''")
        return len(HU_DATE_RE.findall(txt or ""))
    except Exception:
        return 0


def detect_creator_page(page) -> bool:
    try:
        has_podcastok_heading = page.evaluate("""
        () => {
          const els = Array.from(document.querySelectorAll('h1,h2,h3,div,span'))
            .filter(el => el && el.textContent && el.textContent.trim() === 'PODCASTOK');
          return els.some(el => !el.closest('nav') && !el.closest('aside'));
        }
        """)
    except Exception:
        has_podcastok_heading = False

    ep_count_est = estimate_episode_count_by_dates(page)
    return bool(has_podcastok_heading) and ep_count_est < 3


def extract_visible_episode_cards_dom(page) -> list[dict]:
    js = r"""
    () => {
      const reDate = /\b(\d{4})\.\s*(\d{2})\.\s*(\d{2})\.?\b/;
      const reDur = /\b(\d{1,2}:\d{2})(:\d{2})?\b/;

      const out = [];
      const seen = new Set();

      const all = Array.from(document.querySelectorAll('body *'));
      for (const el of all) {
        const txt = (el.innerText || '').trim();
        if (!txt) continue;
        if (!reDate.test(txt)) continue;

        let card = el;
        for (let i=0; i<8 && card; i++) {
          const cTxt = (card.innerText || '');
          if (reDate.test(cTxt) && reDur.test(cTxt)) break;
          card = card.parentElement;
        }
        if (!card) continue;

        const links = Array.from(card.querySelectorAll('a[href]'))
          .map(a => a.href)
          .filter(h => typeof h === 'string' && h.startsWith('http'));

        if (links.length === 0) continue;

        let title = '';
        const lines = (card.innerText || '').split('\n').map(s => s.trim()).filter(Boolean);
        if (lines.length > 0) title = lines[0];

        const m = (card.innerText || '').match(reDate);
        const date = m ? `${m[1]}.${m[2]}.${m[3]}.` : '';

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


def scrape_episode_urls_from_text(text: str) -> set[str]:
    out = set()
    if not text:
        return out

    for m in re.finditer(r"https?://atalon\.hu/[a-z0-9/_\-]+", text, flags=re.IGNORECASE):
        u = m.group(0)
        if "/_next/" in u or "/assets" in u:
            continue
        out.add(u)

    for m in EPISODE_SLUG_RE.finditer(text):
        slug = m.group(1)
        out.add(urljoin(ATALON_BASE, "/" + slug))

    return out


def build_predict_list_max_to_zero(found_urls: set[str], meta_map: dict[str, dict]) -> list[dict]:
    groups = {}
    leftovers = set(found_urls)

    for u in found_urls:
        path = urlparse(u).path.strip("/")
        m = re.match(r"^(.*)-s(\d+)-e(\d+)$", path, flags=re.IGNORECASE)
        if not m:
            continue
        prefix, s_str, e_str = m.group(1), m.group(2), m.group(3)
        key = (prefix.lower(), s_str)
        groups.setdefault(key, {"prefix": prefix, "season": s_str, "max_e": -1, "e_len": 1})
        groups[key]["max_e"] = max(groups[key]["max_e"], int(e_str))
        groups[key]["e_len"] = max(groups[key]["e_len"], len(e_str))
        leftovers.discard(u)

    out = []
    seen = set()

    for _, g in groups.items():
        max_e = g["max_e"]
        if max_e < 0:
            continue
        if max_e > MAX_PREDICT_EP:
            print(f"  - Figyelem: max epizód {max_e}, sapka {MAX_PREDICT_EP}-re vágva")
            max_e = MAX_PREDICT_EP

        e_len = g["e_len"]
        prefix = g["prefix"]
        s_str = g["season"]

        for e in range(max_e, -1, -1):
            ep = str(e).zfill(e_len)
            slug = f"{prefix}-s{s_str}-e{ep}"
            url = urljoin(ATALON_BASE, "/" + slug)

            if url in seen:
                continue
            seen.add(url)

            meta = meta_map.get(url, {})
            out.append({
                "url": url,
                "title": meta.get("title"),
                "date": meta.get("date"),
                "predicted": (url not in found_urls),
            })

    for u in sorted(leftovers):
        if u in seen:
            continue
        seen.add(u)
        meta = meta_map.get(u, {})
        out.append({"url": u, "title": meta.get("title"), "date": meta.get("date"), "predicted": False})

    return out


def collect_episodes_hardcore(show_url: str, headful: bool) -> tuple[str, list[dict]]:
    network_urls: set[str] = set()
    collected_dom: dict[str, dict] = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headful)
        context = browser.new_context()
        page = context.new_page()

        def on_response(resp):
            try:
                ct = (resp.headers.get("content-type") or "").lower()
                u = resp.url.lower()
                if ("application/json" in ct) or u.endswith(".json") or ("increment.php" in u):
                    txt = resp.text()
                    network_urls.update(scrape_episode_urls_from_text(txt))
            except Exception:
                pass

        page.on("response", on_response)

        page.goto(show_url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(1200)
        handle_consent(page, timeout_ms=15000)

        select_all_seasons_if_present(page)

        if detect_creator_page(page):
            page.close()
            browser.close()
            return "creator", []

        expected = max(0, estimate_episode_count_by_dates(page))
        stable = 0
        last_total = -1

        for i in range(260):
            handle_consent(page, timeout_ms=2000)
            if i in (5, 25):
                select_all_seasons_if_present(page)

            for _ in range(4):
                if not click_mutass_tobbet(page):
                    break
                page.wait_for_timeout(700)

            batch = extract_visible_episode_cards_dom(page)
            for it in batch:
                u = (it.get("url") or "").strip()
                if not u or "/_next/" in u or "/assets" in u:
                    continue
                if u not in collected_dom:
                    collected_dom[u] = {
                        "title": (it.get("title") or "").strip() or None,
                        "date": (it.get("date") or "").strip() or None
                    }
                else:
                    if not collected_dom[u].get("title") and (it.get("title") or "").strip():
                        collected_dom[u]["title"] = (it.get("title") or "").strip()
                    if not collected_dom[u].get("date") and (it.get("date") or "").strip():
                        collected_dom[u]["date"] = (it.get("date") or "").strip()

            total = len(collected_dom) + len(network_urls)
            if total == last_total:
                stable += 1
            else:
                stable = 0
                last_total = total

            if expected > 0 and total >= expected and stable >= 7:
                break

            moved = scroll_step(page)
            page.wait_for_timeout(650)

            if stable >= 14 and not moved:
                break

            if i % 20 == 0:
                expected = max(expected, estimate_episode_count_by_dates(page))

        # FAILSAFE PASS 2
        expected_final = max(expected, estimate_episode_count_by_dates(page))
        total_first = len(collected_dom) + len(network_urls)

        if expected_final > 0 and total_first < expected_final:
            scroll_to_top_everywhere(page)
            handle_consent(page, timeout_ms=3000)
            select_all_seasons_if_present(page)

            stable = 0
            last_total = -1

            for _ in range(220):
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
                            "title": (it.get("title") or "").strip() or None,
                            "date": (it.get("date") or "").strip() or None
                        }

                total = len(collected_dom) + len(network_urls)
                if total == last_total:
                    stable += 1
                else:
                    stable = 0
                    last_total = total

                if total >= expected_final and stable >= 7:
                    break

                moved = scroll_step(page)
                page.wait_for_timeout(650)

                if stable >= 14 and not moved:
                    break

        page.close()
        browser.close()

    found_urls = set(collected_dom.keys()) | set(network_urls)
    if not found_urls:
        return "creator", []

    meta_map = {u: {"title": v.get("title"), "date": v.get("date")} for u, v in collected_dom.items()}
    episodes_ordered = build_predict_list_max_to_zero(found_urls, meta_map)
    return "podcast_list", episodes_ordered


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
                        predicted=False
                    )
                    ok_all = ok_all and ok
                else:
                    ptype, episodes = collect_episodes_hardcore(seed, headful=args.headful)

                    if ptype == "creator":
                        print("  - PODCASTOK (creator) oldal -> SKIP (nem jelölöm késznek)")
                        ok_all = False
                    else:
                        print(f"  - Letöltési lista (max->0 predikcióval): {len(episodes)} URL")
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
                                    known_date=ep.get("date"),
                                    predicted=bool(ep.get("predicted"))
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

    #https://atalon.hu/viclondonban