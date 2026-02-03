#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import hashlib
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeoutError

BASE_URL_DEFAULT = "https://podkaszt.hu/adasok/uj/"
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


def init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS episodes (
            episode_key TEXT PRIMARY KEY,
            title TEXT,
            producer TEXT,
            date TEXT,
            status TEXT,
            audio_url TEXT,
            filename TEXT,
            error TEXT,
            added_at TEXT,
            downloaded_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pages (
            page INTEGER PRIMARY KEY,
            scanned_at TEXT
        )
    """)
    conn.commit()
    return conn


def is_page_scanned(conn: sqlite3.Connection, page: int) -> bool:
    return bool(conn.execute("SELECT 1 FROM pages WHERE page=?", (page,)).fetchone())


def mark_page_scanned(conn: sqlite3.Connection, page: int):
    conn.execute("""
        INSERT INTO pages(page, scanned_at) VALUES(?, ?)
        ON CONFLICT(page) DO UPDATE SET scanned_at=excluded.scanned_at
    """, (page, now_utc_iso()))
    conn.commit()


def is_downloaded(conn: sqlite3.Connection, episode_key: str) -> bool:
    row = conn.execute("SELECT status FROM episodes WHERE episode_key=?", (episode_key,)).fetchone()
    return bool(row and row[0] == "downloaded")


def upsert_episode(conn: sqlite3.Connection, episode_key: str, title: str, producer: str, date: str):
    conn.execute("""
        INSERT INTO episodes(episode_key, title, producer, date, status, audio_url, filename, error, added_at, downloaded_at)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(episode_key) DO UPDATE SET
            title=excluded.title,
            producer=excluded.producer,
            date=excluded.date
    """, (episode_key, title, producer, date, "queued", None, None, None, now_utc_iso(), None))
    conn.commit()


def mark_status(conn: sqlite3.Connection, episode_key: str, status: str,
                audio_url: Optional[str] = None, filename: Optional[str] = None, error: Optional[str] = None):
    downloaded_at = now_utc_iso() if status == "downloaded" else None
    conn.execute("""
        UPDATE episodes
        SET status=?,
            audio_url=COALESCE(?, audio_url),
            filename=COALESCE(?, filename),
            error=COALESCE(?, error),
            downloaded_at=COALESCE(?, downloaded_at)
        WHERE episode_key=?
    """, (status, audio_url, filename, error, downloaded_at, episode_key))
    conn.commit()


def accept_cookies_if_present(page) -> bool:
    # main document
    try:
        btn = page.get_by_role("button", name=COOKIE_ACCEPT_REGEX)
        if btn.count() > 0:
            btn.first.click(timeout=2500)
            page.wait_for_timeout(400)
            return True
    except Exception:
        pass

    # iframes
    for frame in page.frames:
        try:
            btn = frame.get_by_role("button", name=COOKIE_ACCEPT_REGEX)
            if btn.count() > 0:
                btn.first.click(timeout=2500)
                page.wait_for_timeout(400)
                return True
        except Exception:
            continue

    return False


def find_table(page):
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
    """
    Used to detect page change after clicking pagination.
    """
    table = find_table(page)
    if not table:
        return ""
    hm = header_map(table)
    title_i, prod_i, date_i = hm["title"], hm["producer"], hm["date"]
    if title_i is None or prod_i is None or date_i is None:
        return ""
    row = table.locator("tbody tr").first
    if row.count() == 0:
        return ""
    tds = row.locator("td")
    try:
        title = tds.nth(title_i).inner_text().strip()
        prod = tds.nth(prod_i).inner_text().strip()
        date = tds.nth(date_i).inner_text().strip()
        return f"{title}|{prod}|{date}"
    except Exception:
        return ""


def get_pagination_scope(page):
    """
    Try to narrow down to the pagination area near "Oldalak: N".
    If not found, fall back to full page.
    """
    label = page.locator("text=/Oldalak:\\s*\\d+/").first
    if label.count() == 0:
        return page

    # Walk up a few ancestors to get a container that includes the page buttons
    node = label
    for _ in range(6):
        try:
            node = node.locator("xpath=..")
            # if it contains clickable elements with numbers, use it
            if node.locator("text=/^2$/").count() > 0:
                return node
            if node.locator("a,button").count() > 0:
                # still might be okay
                pass
        except Exception:
            break

    return page


def click_page_number(page, target_page: int, wait_timeout_ms: int = 20000) -> bool:
    """
    Click the pagination number (2, 3, 4...) reliably.
    Strategy:
      A) Try accessible button/link/role=button with exact visible text
      B) Fallback: find any element with exact text and click the one near the top of the page
    Then wait until table content changes.
    """
    before = get_first_row_signature(page)
    target = str(target_page)

    # --- A) Accessible / semantic tries ---
    tries = []
    try:
        tries.append(page.get_by_role("button", name=target))
    except Exception:
        pass
    try:
        tries.append(page.get_by_role("link", name=target))
    except Exception:
        pass

    # Playwright text-is (exact match), often works when the number is inside a <button>
    tries.append(page.locator(f"button:has(:text-is('{target}'))"))
    tries.append(page.locator(f"a:has(:text-is('{target}'))"))
    tries.append(page.locator(f"[role=button]:has(:text-is('{target}'))"))

    for loc in tries:
        try:
            if loc.count() > 0:
                el = loc.first
                el.scroll_into_view_if_needed()
                el.click(timeout=3000)
                # wait for change
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

    # --- B) Bounding-box fallback (click the '2' that's near the top, i.e. pagination) ---
    candidates = page.locator(f":text-is('{target}')")
    cnt = candidates.count()
    if cnt == 0:
        return False

    best = None  # (y, locator)
    for i in range(min(cnt, 80)):  # cap scanning to avoid huge loops
        try:
            el = candidates.nth(i)
            if not el.is_visible():
                continue
            box = el.bounding_box()
            if not box:
                continue

            # Pagination is near the top center; table rows are much lower.
            # Tune this threshold if needed.
            if box["y"] > 250:
                continue

            # Prefer small-ish boxes (pagination buttons), not big containers
            if box["width"] > 200 or box["height"] > 120:
                continue

            # Try to click a clickable ancestor (button/link/role=button), else click itself
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
            best = (box["y"], target_el) if best is None else (min(best[0], box["y"]), target_el if box["y"] < best[0] else best[1])
        except Exception:
            continue

    if not best:
        return False

    try:
        best[1].scroll_into_view_if_needed()
        best[1].click(timeout=3000)
    except Exception:
        return False

    # wait for table change
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


def download_with_resume(session: requests.Session, url: str, out_path: Path, referer: str):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".part")

    existing = tmp.stat().st_size if tmp.exists() else 0
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; PodkasztUIDownloader/3.0)",
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
    ap.add_argument("--base-url", default=BASE_URL_DEFAULT)
    ap.add_argument("--max-pages", type=int, default=3)
    ap.add_argument("--start-page", type=int, default=1)
    ap.add_argument("--out", default="podcasts")
    ap.add_argument("--db", default="podkaszt_ui.sqlite")
    ap.add_argument("--profile", default=".pw-profile")
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--slowmo", type=int, default=0)
    ap.add_argument("--force-rescan", action="store_true")
    ap.add_argument("--audio-wait", type=float, default=12.0)
    args = ap.parse_args()

    base_url = args.base_url if args.base_url.endswith("/") else (args.base_url + "/")
    out_dir = Path(args.out)
    db_path = Path(args.db)
    profile_dir = Path(args.profile)

    conn = init_db(db_path)

    print(f"[i] Base:    {base_url}")
    print(f"[i] DB:      {db_path.resolve()}")
    print(f"[i] Out:     {out_dir.resolve()}")
    print(f"[i] Profile: {profile_dir.resolve()}")

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=(not args.headful),
            accept_downloads=True,
            slow_mo=args.slowmo if args.slowmo > 0 else None,
        )
        page = context.pages[0] if context.pages else context.new_page()

        session = requests.Session()

        # Open page 1
        page.goto(base_url, wait_until="domcontentloaded")
        page.wait_for_timeout(800)
        if accept_cookies_if_present(page):
            print("[i] Cookie consent accepted.")

        # If start-page > 1, click through pages sequentially until reaching it
        current = 1
        while current < args.start_page:
            ok = click_page_number(page, current + 1)
            if not ok:
                raise RuntimeError(f"Could not click pagination to reach page {current+1}")
            accept_cookies_if_present(page)
            current += 1

        # Main loop: process page_no, then click to next page number
        for page_no in range(args.start_page, args.max_pages + 1):
            if (not args.force_rescan) and is_page_scanned(conn, page_no):
                print(f"[i] Page {page_no}: already scanned, skipping")
            else:
                accept_cookies_if_present(page)

                table = find_table(page)
                if not table:
                    print(f"[!] Page {page_no}: table not found.")
                    break

                hm = header_map(table)
                title_i, prod_i, date_i = hm["title"], hm["producer"], hm["date"]
                if title_i is None or prod_i is None or date_i is None:
                    print(f"[!] Page {page_no}: header map failed. Headers: {hm['headers']}")
                    break

                rows = table.locator("tbody tr")
                n = rows.count()
                print(f"[i] Page {page_no}: rows={n}")

                for r in range(n):
                    row = rows.nth(r)
                    tds = row.locator("td")
                    if tds.count() <= max(title_i, prod_i, date_i):
                        continue

                    title = tds.nth(title_i).inner_text().strip()
                    producer = tds.nth(prod_i).inner_text().strip()
                    date = tds.nth(date_i).inner_text().strip()

                    if not title or not producer or not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
                        continue

                    episode_key = sha1(f"{title}|{producer}|{date}")
                    upsert_episode(conn, episode_key, title, producer, date)

                    if is_downloaded(conn, episode_key):
                        continue

                    # Select episode by clicking title
                    try:
                        tds.nth(title_i).click(timeout=2500)
                    except Exception as e:
                        mark_status(conn, episode_key, "error", error=f"title_click_failed: {e}")
                        continue

                    # Start playback: clicking the first column usually triggers play (icon column)
                    try:
                        tds.nth(0).click(timeout=1500)
                    except Exception:
                        pass

                    page.wait_for_timeout(600)

                    # Read currentSrc and download it (equivalent to the native Download menu)
                    audio_url = get_audio_url_from_player(page, wait_s=args.audio_wait)
                    if not audio_url:
                        mark_status(conn, episode_key, "no_audio", error="No audio currentSrc/src found (blob/stream?).")
                        print(f"[!] no_audio: {producer} | {title} | {date}")
                        continue

                    ext = guess_ext_from_url(audio_url)
                    filename = f"{slugify(producer)} - {slugify(title)} - {date} [{episode_key[:10]}]{ext}"
                    out_path = out_dir / filename

                    if out_path.exists() and out_path.stat().st_size > 0:
                        mark_status(conn, episode_key, "downloaded", audio_url=audio_url, filename=str(out_path))
                        continue

                    try:
                        print(f"[+] downloading: {filename}")
                        download_with_resume(session, audio_url, out_path, referer=base_url)
                        mark_status(conn, episode_key, "downloaded", audio_url=audio_url, filename=str(out_path))
                    except Exception as e:
                        mark_status(conn, episode_key, "error", audio_url=audio_url, error=str(e))
                        print(f"[!] download error: {e}")

                    page.wait_for_timeout(250)

                mark_page_scanned(conn, page_no)

            # Move to next page by clicking the next number
            if page_no < args.max_pages:
                ok = click_page_number(page, page_no + 1)
                if not ok:
                    raise RuntimeError(f"Could not click pagination number {page_no+1}")
                page.wait_for_timeout(500)

        context.close()

    print("[i] Done.")


if __name__ == "__main__":
    main()
