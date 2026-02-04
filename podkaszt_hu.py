#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import hashlib
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeoutError

BASE_URL_DEFAULT = "https://podkaszt.hu/adasok/uj/" # podcastok 20 oldalanként 3500 oldal
AUDIO_EXTS = (".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav")

COOKIE_ACCEPT_REGEX = re.compile(r"(Beleegyezés|Elfogadom|Accept|Agree|Allow)", re.I)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def slugify(name: str, max_len: int = 180) -> str:
    name = (name or "").strip()
    name = re.sub(r"\s+", " ", name)
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name)
    name = name.strip(" ._")
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
                        timeout=wait_timeout_ms
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
            timeout=wait_timeout_ms
        )
    except Exception:
        page.wait_for_timeout(900)

    return True


def get_audio_url_from_player(page, wait_s: float = 12.0) -> Optional[str]:
    try:
        page.wait_for_function(
            "() => { const m=document.querySelector('audio,video'); return m && (m.currentSrc || m.src) && (m.currentSrc || m.src).length>0; }",
            timeout=int(wait_s * 1000)
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
    tmp = out_path.with_suffix(out_path.suffix + ".part")

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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default=BASE_URL_DEFAULT) #alap url atallitas, hatha megvaltozik a jövöben az url
    ap.add_argument("--max-pages", type=int, default=2) #max letoltott oldalak szama
    ap.add_argument("--start-page", type=int, default=1) #kezdő oldal, hogy ne kelljen mindig 1-től induljon
    ap.add_argument("--out", default="podcasts") #letöltési mapp a podcastoknak
    ap.add_argument("--visited", default="podkaszt_visited.txt") #visited file a podcastoknak
    ap.add_argument("--profile", default=".pw-profile", help="Persistent browser profile folder (stores cookie consent)")
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--slowmo", type=int, default=0, help="ms slow motion for debugging (e.g. 150)")
    ap.add_argument("--audio-wait", type=float, default=12.0)
    ap.add_argument("--retries", type=int, default=3, help="How many times to attempt getting an audio URL per episode")
    args = ap.parse_args()

    base_url = args.base_url if args.base_url.endswith("/") else (args.base_url + "/")
    out_dir = Path(args.out)
    visited_path = Path(args.visited)
    profile_dir = Path(args.profile)

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
        while current < args.start_page:
            ok = click_page_number(page, current + 1)
            if not ok:
                raise RuntimeError(f"Could not click pagination number {current+1} to reach start-page")
            accept_cookies_if_present(page)
            page.wait_for_timeout(600)
            current += 1

        # Process pages start_page..max_pages
        for page_no in range(args.start_page, args.max_pages + 1):
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

                # Also skip if file already exists (and mark visited)
                # (filename depends on url ext, but we can only know ext after audio_url;
                # still, if user previously downloaded with same naming scheme, visited file will have it.)
                # We'll proceed normally.

                # Select by clicking title
                try:
                    tds.nth(ti).click(timeout=2500)
                except Exception as e:
                    print(f"[!] select failed: {producer} | {title} | {date} -> {e}")
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
                    print(f"[!] no_audio: {producer} | {title} | {date}")
                    continue

                # Sync cookies before downloading (important for some hosts)
                try:
                    sync_cookies_to_requests(context, session)
                except Exception:
                    pass

                ext = guess_ext_from_url(audio_url)
                filename = f"{slugify(producer)} - {slugify(title)} - {date} [{episode_key[:10]}]{ext}"
                out_path = out_dir / filename

                # If already exists, mark visited and skip
                if out_path.exists() and out_path.stat().st_size > 0:
                    visited.add(episode_key)
                    append_visited(visited_path, episode_key, date, producer, title)
                    print(f"[i] already exists -> visited: {out_path.name}")
                    continue

                try:
                    print(f"[+] downloading: {out_path.name}")
                    download_with_resume(session, audio_url, out_path, referer=base_url)
                    visited.add(episode_key)
                    append_visited(visited_path, episode_key, date, producer, title)
                except Exception as e:
                    print(f"[!] download error: {producer} | {title} | {date} -> {e}")

                page.wait_for_timeout(250)

            # go next page by clicking the next number
            if page_no < args.max_pages:
                ok = click_page_number(page, page_no + 1)
                if not ok:
                    raise RuntimeError(f"Could not click pagination number {page_no+1}")
                page.wait_for_timeout(700)

        context.close()

    print("[i] Done.")


if __name__ == "__main__":
    main()
