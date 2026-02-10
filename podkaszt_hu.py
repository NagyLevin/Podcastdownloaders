import argparse
import hashlib
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeoutError

BASE_URL_DEFAULT = "https://podkaszt.hu/adasok/uj/"  # podcastok 20 oldalanként 3500 oldal
AUDIO_EXTS = (".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav")
TMP_SUFFIX = ".part"

COOKIE_ACCEPT_REGEX = re.compile(r"(Beleegyezés|Elfogadom|Accept|Agree|Allow)", re.I)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def slugify(name: str, max_len: int = 4000) -> str:
    """
    Fájlnévben nem megengedett karakterek cseréje.
    Fontos: itt NEM a fájlrendszer-limitet kezeljük, csak tisztítunk.
    A tényleges hossz/byte-limitet build_safe_filename() intézi, '___' suffix-szel.
    """
    name = (name or "").strip()
    name = re.sub(r"\s+", " ", name)
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name)
    name = name.strip(" ._")

    # csak egy "védőkorlát", hogy ne nőjön végtelenre a string
    if len(name) > max_len:
        name = name[:max_len].rstrip(" ._")
    return name or "untitled"


def load_visited(path: Path) -> set[str]:
    visited = set()
    if not path.exists():
        return visited
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # format: key<TAB>date<TAB>producer<TAB>title
            key = line.split("\t", 1)[0].strip()
            if key:
                visited.add(key)
    return visited


def append_visited(path: Path, episode_key: str, date: str, producer: str, title: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    line = f"{episode_key}\t{date}\t{producer}\t{title}\n"
    with open(path, "a", encoding="utf-8") as f:
        f.write(line)


def append_timeout(path: Path, filename: str, date: str, producer: str, title: str, episode_key: str, reason: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    reason = (reason or "").replace("\n", " ").replace("\r", " ").strip()
    line = f"{filename}\t{date}\t{producer}\t{title}\t{episode_key}\t{reason}\n"
    with open(path, "a", encoding="utf-8") as f:
        f.write(line)


def accept_cookies_if_present(page) -> bool:
    # main document
    try:
        btn = page.get_by_role("button", name=COOKIE_ACCEPT_REGEX)
        if btn.count() > 0:
            btn.first.click(timeout=3000)
            page.wait_for_timeout(400)
            return True
    except Exception:
        pass

    # iframes
    for frame in page.frames:
        try:
            btn = frame.get_by_role("button", name=COOKIE_ACCEPT_REGEX)
            if btn.count() > 0:
                btn.first.click(timeout=3000)
                page.wait_for_timeout(400)
                return True
        except Exception:
            continue

    return False


def find_table(page):
    # Prefer a table with headers containing "Cím" and "Előadó"
    tables = page.locator("table")
    for i in range(tables.count()):
        t = tables.nth(i)
        try:
            header_text = " ".join(t.locator("th").all_text_contents())
            if ("Cím" in header_text) and ("Előadó" in header_text):
                return t
        except Exception:
            continue
    return tables.first if tables.count() > 0 else None


def header_map(table):
    ths = table.locator("th")
    headers = [h.strip() for h in ths.all_text_contents()]

    def idx(exact: str) -> Optional[int]:
        for i, h in enumerate(headers):
            if h.strip().lower() == exact.lower():
                return i
        return None

    return {
        "headers": headers,
        "title": idx("Cím"),
        "producer": idx("Előadó"),
        "date": idx("Dátum"),
    }


def get_first_row_signature(page) -> str:
    table = find_table(page)
    if not table:
        return ""
    hm = header_map(table)
    ti, pi, di = hm["title"], hm["producer"], hm["date"]
    if ti is None or pi is None or di is None:
        return ""
    row = table.locator("tbody tr").first
    if row.count() == 0:
        return ""
    tds = row.locator("td")
    try:
        title = tds.nth(ti).inner_text().strip()
        prod = tds.nth(pi).inner_text().strip()
        date = tds.nth(di).inner_text().strip()
        return f"{title}|{prod}|{date}"
    except Exception:
        return ""


def click_page_number(page, target_page: int, wait_timeout_ms: int = 20000) -> bool:
    """
    Click pagination number (2,3,4...) robustly:
    - tries semantic button/link
    - fallback: click the 'N' element near top of page (pagination), not table row index
    """
    before = get_first_row_signature(page)
    target = str(target_page)

    tries = []
    try:
        tries.append(page.get_by_role("button", name=target))
    except Exception:
        pass
    try:
        tries.append(page.get_by_role("link", name=target))
    except Exception:
        pass

    tries.append(page.locator(f"button:has(:text-is('{target}'))"))
    tries.append(page.locator(f"a:has(:text-is('{target}'))"))
    tries.append(page.locator(f"[role=button]:has(:text-is('{target}'))"))

    for loc in tries:
        try:
            if loc.count() > 0:
                el = loc.first
                el.scroll_into_view_if_needed()
                el.click(timeout=3000)
                try:
                    page.wait_for_function(
                        "(prev) => {"
                        "  const t=document.querySelector('table');"
                        "  if(!t) return false;"
                        "  const r=t.querySelector('tbody tr');"
                        "  if(!r) return false;"
                        "  const txt=r.innerText || '';"
                        "  return txt.length>0 && txt !== prev;"
                        "}",
                        before,
                        timeout=wait_timeout_ms,
                    )
                except Exception:
                    page.wait_for_timeout(900)
                return True
        except Exception:
            continue

    # Fallback: find exact text near top
    candidates = page.locator(f":text-is('{target}')")
    cnt = candidates.count()
    if cnt == 0:
        return False

    best = None  # (y, locator)
    for i in range(min(cnt, 80)):
        try:
            el = candidates.nth(i)
            if not el.is_visible():
                continue
            box = el.bounding_box()
            if not box:
                continue
            # pagination is near top; ignore table rows lower down
            if box["y"] > 260:
                continue
            if box["width"] > 200 or box["height"] > 120:
                continue

            clickable = None
            for xpath in [
                "xpath=ancestor-or-self::button[1]",
                "xpath=ancestor-or-self::a[1]",
                "xpath=ancestor-or-self::*[@role='button'][1]",
            ]:
                anc = el.locator(xpath)
                if anc.count() > 0 and anc.first.is_visible():
                    clickable = anc.first
                    break

            target_el = clickable or el
            if best is None or box["y"] < best[0]:
                best = (box["y"], target_el)
        except Exception:
            continue

    if not best:
        return False

    try:
        best[1].scroll_into_view_if_needed()
        best[1].click(timeout=3000)
    except Exception:
        return False

    try:
        page.wait_for_function(
            "(prev) => {"
            "  const t=document.querySelector('table');"
            "  if(!t) return false;"
            "  const r=t.querySelector('tbody tr');"
            "  if(!r) return false;"
            "  const txt=r.innerText || '';"
            "  return txt.length>0 && txt !== prev;"
            "}",
            before,
            timeout=wait_timeout_ms,
        )
    except Exception:
        page.wait_for_timeout(900)

    return True


def try_goto_page_by_url(page, base_url: str, target_page: int) -> bool:
    """
    URL-es fallback, ha a pagination kattintás nem működik.
    Több gyakori mintát kipróbál.
    """
    before = get_first_row_signature(page)

    candidates = [
        f"{base_url}?page={target_page}",
        f"{base_url}?p={target_page}",
        f"{base_url}{target_page}/",
        f"{base_url}page/{target_page}/",
        f"{base_url}oldal/{target_page}/",
    ]

    for u in candidates:
        try:
            page.goto(u, wait_until="domcontentloaded", timeout=25000)
            page.wait_for_timeout(700)
            accept_cookies_if_present(page)

            after = get_first_row_signature(page)
            if after and after != before:
                return True
        except Exception:
            continue

    return False


def get_audio_url_from_player(page, wait_s: float = 12.0) -> Optional[str]:
    try:
        page.wait_for_function(
            "() => { const m=document.querySelector('audio,video'); return m && (m.currentSrc || m.src) && (m.currentSrc || m.src).length>0; }",
            timeout=int(wait_s * 1000),
        )
        url = page.evaluate(
            "() => { const m=document.querySelector('audio,video'); return m ? (m.currentSrc || m.src) : null; }"
        )
        if not url:
            return None
        url = str(url).strip()
        if url.startswith("blob:"):
            return None
        return url
    except PwTimeoutError:
        return None


def guess_ext_from_url(url: str) -> str:
    path = urlparse(url).path
    ext = os.path.splitext(path)[1].lower()
    return ext if ext in AUDIO_EXTS else ".mp3"


def sync_cookies_to_requests(context, session: requests.Session):
    jar = requests.cookies.RequestsCookieJar()
    for c in context.cookies():
        jar.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path", "/"))
    session.cookies = jar


def download_with_resume(session: requests.Session, url: str, out_path: Path, referer: str):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + TMP_SUFFIX)

    existing = tmp.stat().st_size if tmp.exists() else 0
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; PodkasztVisitedDownloader/1.0)",
        "Referer": referer,
    }
    if existing > 0:
        headers["Range"] = f"bytes={existing}-"

    r = session.get(url, stream=True, timeout=60, allow_redirects=True, headers=headers)
    mode = "ab" if (existing > 0 and r.status_code == 206) else "wb"
    if mode == "wb" and tmp.exists():
        tmp.unlink(missing_ok=True)

    r.raise_for_status()

    with open(tmp, mode) as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)

    tmp.replace(out_path)


# --------- ÚJ: fájlnév-hossz kezelése (byte-alapon) ---------

def get_name_max(dir_path: Path, fallback: int = 255) -> int:
    """
    Filesystem limit for a single filename component (NAME_MAX).
    Tipikusan 255 byte Linuxon.
    """
    try:
        return int(os.pathconf(str(dir_path), "PC_NAME_MAX"))
    except Exception:
        return fallback


def get_path_max(dir_path: Path, fallback: int = 4096) -> int:
    """
    Filesystem limit for full path length (PATH_MAX).
    Tipikusan 4096 byte Linuxon.
    """
    try:
        return int(os.pathconf(str(dir_path), "PC_PATH_MAX"))
    except Exception:
        return fallback


def trim_utf8_to_bytes(text: str, max_bytes: int, suffix: str = "___") -> str:
    """
    Trim string so that utf-8 byte length <= max_bytes.
    If trimming happens, append suffix (also counted in max_bytes).
    """
    if max_bytes <= 0:
        return ""

    b = (text or "").encode("utf-8", errors="ignore")
    if len(b) <= max_bytes:
        return text or ""

    suf_b = suffix.encode("utf-8", errors="ignore")
    if len(suf_b) >= max_bytes:
        # Edge-case: suffix sem fér be; best effort
        return suf_b[:max_bytes].decode("utf-8", errors="ignore")

    cut = max_bytes - len(suf_b)
    trimmed = b[:cut].decode("utf-8", errors="ignore")
    trimmed = trimmed.rstrip(" ._-")
    return trimmed + suffix


def build_safe_filename(out_dir: Path, producer: str, title: str, date: str, episode_key: str, ext: str) -> str:
    """
    Olyan fájlnevet épít, ami:
    - tisztított (slugify)
    - NAME_MAX-ot (filename komponens) nem lépi túl byte-ban
    - ha vágni kell, '___' kerül a végére (kiterjesztés elé)
    - plusz figyel a PATH_MAX-ra is (ritkább, de létező gond)
    """
    ext = (ext or "").lower()
    if ext not in AUDIO_EXTS:
        ext = ".mp3"

    # Slugify (itt nem a limit a lényeg, hanem a tisztítás)
    producer_s = slugify(producer, max_len=4000)
    title_s = slugify(title, max_len=8000)

    key10 = (episode_key or "")[:10]
    stem = f"{producer_s} - {title_s} - {date} [{key10}]"

    name_max = get_name_max(out_dir)
    tail_bytes = len((ext + TMP_SUFFIX).encode("utf-8", errors="ignore"))
    max_stem_bytes = max(1, name_max - tail_bytes)

    safe_stem = trim_utf8_to_bytes(stem, max_stem_bytes, suffix="___")
    filename = safe_stem + ext

    # EXTRA: PATH_MAX védelem (teljes útvonal hossz)
    # Ha a teljes path mégis túl hosszú lenne (nagyon hosszú out_dir esetén), vágunk tovább.
    path_max = get_path_max(out_dir)
    full_path_bytes = len(str(out_dir / filename).encode("utf-8", errors="ignore"))
    full_tmp_path_bytes = len(str(out_dir / (filename + TMP_SUFFIX)).encode("utf-8", errors="ignore"))
    if full_path_bytes > path_max or full_tmp_path_bytes > path_max:
        # Mennyi férne bele a stem-ből?
        # approx: path_max - len(out_dir + os.sep) - len(ext)
        prefix_bytes = len((str(out_dir) + os.sep).encode("utf-8", errors="ignore"))
        max_filename_bytes = max(1, path_max - prefix_bytes)
        max_stem_bytes2 = max(1, max_filename_bytes - tail_bytes)
        safe_stem2 = trim_utf8_to_bytes(stem, max_stem_bytes2, suffix="___")
        filename = safe_stem2 + ext

    return filename


def halve_filename_fallback(out_dir: Path, filename: str) -> str:
    stem, ext = os.path.splitext(filename)
    if not ext:
        ext = ".mp3"
    if ext.lower() not in AUDIO_EXTS:
        ext = ".mp3"

    # ha már volt rajta jelölés, ne halmozzuk
    base_stem = stem
    if base_stem.endswith("___"):
        base_stem = base_stem[:-3].rstrip(" ._-")

    b = base_stem.encode("utf-8", errors="ignore")
    half_bytes = max(1, len(b) // 2)

    name_max = get_name_max(out_dir)
    tail_bytes = len((ext + TMP_SUFFIX).encode("utf-8", errors="ignore"))
    max_stem_bytes = max(1, name_max - tail_bytes)

    target_bytes = min(half_bytes, max_stem_bytes)
    safe_stem = trim_utf8_to_bytes(base_stem, target_bytes, suffix="___")
    new_filename = safe_stem + ext

    path_max = get_path_max(out_dir)
    full_path_bytes = len(str(out_dir / new_filename).encode("utf-8", errors="ignore"))
    full_tmp_path_bytes = len(str(out_dir / (new_filename + TMP_SUFFIX)).encode("utf-8", errors="ignore"))
    if full_path_bytes > path_max or full_tmp_path_bytes > path_max:
        prefix_bytes = len((str(out_dir) + os.sep).encode("utf-8", errors="ignore"))
        max_filename_bytes = max(1, path_max - prefix_bytes)
        max_stem_bytes2 = max(1, max_filename_bytes - tail_bytes)
        target_bytes2 = min(target_bytes, max_stem_bytes2)
        safe_stem2 = trim_utf8_to_bytes(base_stem, target_bytes2, suffix="___")
        new_filename = safe_stem2 + ext

    return new_filename


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default=BASE_URL_DEFAULT,help="base url of the podcast website")  # alap url atallitas, hatha megvaltozik a jövöben az url
    ap.add_argument("--start-page", type=int, default=1, help="start page of the podcast website")  # kezdő oldal, hogy ne kelljen mindig 1-től induljon
    ap.add_argument("--end-page", type=int, default=2, help="end page of the podcast website")  # max letoltott oldalak szama
    ap.add_argument("--out", default="podcasts", help="output directory for the podcast files")  # letöltési mappa a podcastoknak
    ap.add_argument("--visited", default="podkaszt_visited.txt", help="visited file for the podcast files")  # visited file a podcastoknak
    ap.add_argument("--profile", default=".pw-profile", help="Persistent browser profile folder (stores cookie consent)")
    ap.add_argument("--headful", action="store_true", help="show the browser window for debugging")
    ap.add_argument("--slowmo", type=int, default=0, help="ms slow motion for debugging (e.g. 150)")
    ap.add_argument("--audio-wait", type=float, default=12.0, help="wait time for the audio url")
    ap.add_argument("--retries", type=int, default=3, help="How many times to attempt getting an audio URL per episode")
    args = ap.parse_args()

    base_url = args.base_url if args.base_url.endswith("/") else (args.base_url + "/")
    out_dir = Path(args.out)
    visited_path = Path(args.visited)
    profile_dir = Path(args.profile)
    timeouts_path = Path("timeouts.txt")

    # fontos: out_dir létezzen, különben a pathconf néha nem megbízható
    out_dir.mkdir(parents=True, exist_ok=True)

    visited = load_visited(visited_path)

    print(f"[i] Base:    {base_url}")
    print(f"[i] Out:     {out_dir.resolve()}")
    print(f"[i] Visited: {visited_path.resolve()} (loaded {len(visited)})")
    print(f"[i] Profile: {profile_dir.resolve()}")

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=(not args.headful),
            accept_downloads=True,
            slow_mo=args.slowmo if args.slowmo > 0 else None,
        )
        page = context.pages[0] if context.pages else context.new_page()

        # capture audio URLs from network responses (fallback)
        last_audio_from_net = {"url": None}

        def on_response(resp):
            try:
                u = resp.url
                base = u.lower().split("?")[0]
                if any(base.endswith(ext) for ext in AUDIO_EXTS):
                    last_audio_from_net["url"] = u
            except Exception:
                pass

        page.on("response", on_response)

        session = requests.Session()

        # Open base page & accept cookies
        page.goto(base_url, wait_until="domcontentloaded")
        page.wait_for_timeout(900)
        if accept_cookies_if_present(page):
            print("[i] Cookie consent accepted.")

        # Navigate to start page by clicking numbers (1 -> 2 -> ... -> start_page)
        current = 1
        if args.start_page > 1:
            print(f"[i] skipping to start-page {args.start_page} ...")

        while current < args.start_page:
            target = current + 1
            ok = click_page_number(page, target)
            if not ok:
                ok = try_goto_page_by_url(page, base_url, target)

            if not ok:
                # retry párszor, mert nagy oldalszámnál random szokott lenni
                retry_ok = False
                for _ in range(3):
                    page.wait_for_timeout(1200)
                    ok2 = click_page_number(page, target)
                    if not ok2:
                        ok2 = try_goto_page_by_url(page, base_url, target)
                    if ok2:
                        retry_ok = True
                        break
                if not retry_ok:
                    raise RuntimeError(f"Could not click pagination number {target} to reach start-page")

            accept_cookies_if_present(page)
            page.wait_for_timeout(350)
            current = target
            print(f"skipped to page {current}")

        # Process pages start_page..end_page
        for page_no in range(args.start_page, args.end_page + 1):
            accept_cookies_if_present(page)
            page.wait_for_timeout(300)

            table = find_table(page)
            if not table:
                raise RuntimeError(f"Table not found on page {page_no}")

            hm = header_map(table)
            ti, pi, di = hm["title"], hm["producer"], hm["date"]
            if ti is None or pi is None or di is None:
                raise RuntimeError(f"Header map failed on page {page_no}. Headers: {hm['headers']}")

            rows = table.locator("tbody tr")
            n = rows.count()
            print(f"[i] Page {page_no}: rows={n}")

            page_candidates = 0
            page_ok = 0

            for r in range(n):
                row = rows.nth(r)
                tds = row.locator("td")
                if tds.count() <= max(ti, pi, di):
                    continue

                title = tds.nth(ti).inner_text().strip()
                producer = tds.nth(pi).inner_text().strip()
                date = tds.nth(di).inner_text().strip()

                if not title or not producer or not date:
                    continue

                episode_key = sha1(f"{title}|{producer}|{date}")

                # Skip if visited
                if episode_key in visited:
                    continue

                page_candidates += 1

                # Select by clicking title
                try:
                    tds.nth(ti).click(timeout=2500)
                except Exception as e:
                    reason = f"select failed: {e}"
                    print(f"[!] failed: {producer} | {title} | {date} -> {reason}")
                    filename_nf = build_safe_filename(out_dir, producer, title, date, episode_key, ".mp3")
                    append_timeout(timeouts_path, filename_nf, date, producer, title, episode_key, reason)
                    continue

                # Try to start playback by clicking the icon column
                try:
                    tds.nth(0).click(timeout=1500)
                except Exception:
                    pass

                page.wait_for_timeout(700)

                # Try to obtain audio URL (currentSrc + network), with retries
                audio_url = None
                last_audio_from_net["url"] = None

                for attempt in range(1, max(1, args.retries) + 1):
                    audio_url = get_audio_url_from_player(page, wait_s=args.audio_wait)
                    if audio_url and not audio_url.startswith("blob:"):
                        break

                    # network fallback
                    if last_audio_from_net["url"]:
                        audio_url = last_audio_from_net["url"]
                        break

                    # nudge playback again
                    try:
                        tds.nth(0).click(timeout=1500)
                    except Exception:
                        pass
                    page.wait_for_timeout(500)

                if not audio_url:
                    reason = "no_audio"
                    print(f"[!] failed: {producer} | {title} | {date} -> {reason}")
                    filename_nf = build_safe_filename(out_dir, producer, title, date, episode_key, ".mp3")
                    append_timeout(timeouts_path, filename_nf, date, producer, title, episode_key, reason)
                    continue

                # Sync cookies before downloading (important for some hosts)
                try:
                    sync_cookies_to_requests(context, session)
                except Exception:
                    pass

                ext = guess_ext_from_url(audio_url)

                # ÚJ: fájlnév biztonságos építése (ha túl hosszú, '___'-t kap)
                filename = build_safe_filename(out_dir, producer, title, date, episode_key, ext)
                out_path = out_dir / filename

                # If already exists, mark visited and skip
                exists_nonempty = False
                for _fs_try in range(6):
                    try:
                        exists_nonempty = out_path.exists() and out_path.stat().st_size > 0
                        break
                    except OSError as e:
                        if getattr(e, "errno", None) == 36:
                            print("fallback: fajlnev tul hosszu, rovidites")
                            filename = halve_filename_fallback(out_dir, filename)
                            out_path = out_dir / filename
                            continue
                        raise

                if exists_nonempty:
                    visited.add(episode_key)
                    append_visited(visited_path, episode_key, date, producer, title)
                    print(f"[i] already exists -> visited: {out_path.name}")
                    page_ok += 1
                    continue

                downloaded_ok = False
                last_err = ""
                for _fs_try in range(6):
                    try:
                        print(f"[+] downloading: {out_path.name}")
                        download_with_resume(session, audio_url, out_path, referer=base_url)
                        visited.add(episode_key)
                        append_visited(visited_path, episode_key, date, producer, title)
                        downloaded_ok = True
                        break
                    except OSError as e:
                        if getattr(e, "errno", None) == 36:
                            print("fallback: fajlnev tul hosszu, rovidites")
                            filename = halve_filename_fallback(out_dir, filename)
                            out_path = out_dir / filename
                            continue
                        last_err = f"OSError: {e}"
                        break
                    except Exception as e:
                        last_err = str(e)
                        break

                if downloaded_ok:
                    page_ok += 1
                else:
                    reason = last_err or "download failed"
                    print(f"[!] failed: {producer} | {title} | {date} -> {reason}")
                    append_timeout(timeouts_path, out_path.name, date, producer, title, episode_key, reason)

                page.wait_for_timeout(250)

            print(f"letoltve {page_ok}/{page_candidates}")

            # go next page by clicking the next number
            if page_no < args.end_page:
                ok = click_page_number(page, page_no + 1)
                if not ok:
                    ok = try_goto_page_by_url(page, base_url, page_no + 1)
                if not ok:
                    # retry párszor
                    retry_ok = False
                    for _ in range(3):
                        page.wait_for_timeout(1200)
                        ok2 = click_page_number(page, page_no + 1)
                        if not ok2:
                            ok2 = try_goto_page_by_url(page, base_url, page_no + 1)
                        if ok2:
                            retry_ok = True
                            break
                    if not retry_ok:
                        raise RuntimeError(f"Could not click pagination number {page_no+1}")
                page.wait_for_timeout(700)

        context.close()

    print("[i] Done.")


if __name__ == "__main__":
    main()

#TODO írja az időt majd hogy mikor kezdte el a letöltést meg hogy meddig csinálta a letöltést, esetleg egy gráfot ami mutatja hogy loadol lefelé