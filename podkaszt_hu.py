#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import hashlib
import os
import random
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import feedparser
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

AUDIO_EXTS = (".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav")
DEFAULT_BASE_URL = "https://podkaszt.hu/adasok/uj/"


def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()


def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; PodkasztDownloader/1.2)",
        "Accept-Language": "hu,en;q=0.8",
    })
    retry = Retry(
        total=5,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def init_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS episodes (
            episode_url TEXT PRIMARY KEY,
            producer_url TEXT,
            title TEXT,
            producer TEXT,
            date TEXT,
            audio_url TEXT,
            status TEXT,
            filename TEXT,
            error TEXT,
            added_at TEXT,
            downloaded_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pages (
            page INTEGER PRIMARY KEY,
            listing_url TEXT,
            scanned_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS feeds (
            producer_url TEXT PRIMARY KEY,
            rss_url TEXT,
            status TEXT,
            error TEXT,
            updated_at TEXT
        )
    """)
    conn.commit()
    return conn


def meta_get(conn, k):
    conn.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)")
    row = conn.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    return row[0] if row else None


def meta_set(conn, k, v):
    conn.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)")
    conn.execute("""
        INSERT INTO meta(k, v) VALUES(?, ?)
        ON CONFLICT(k) DO UPDATE SET v=excluded.v
    """, (k, v))
    conn.commit()


def slugify(name: str, max_len: int = 180) -> str:
    name = name.strip()
    name = re.sub(r"\s+", " ", name)
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name)
    name = name.strip(" ._")
    if len(name) > max_len:
        name = name[:max_len].rstrip(" ._")
    return name or "untitled"


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def polite_sleep(base_delay: float):
    if base_delay <= 0:
        return
    jitter = base_delay * 0.3
    time.sleep(base_delay + random.uniform(-jitter, jitter))


def fetch_html(session: requests.Session, url: str, timeout: int = 30) -> str:
    r = session.get(url, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r.text


def is_page_scanned(conn, page: int) -> bool:
    row = conn.execute("SELECT 1 FROM pages WHERE page=?", (page,)).fetchone()
    return bool(row)


def mark_page_scanned(conn, page: int, listing_url: str):
    conn.execute("""
        INSERT INTO pages(page, listing_url, scanned_at)
        VALUES(?, ?, ?)
        ON CONFLICT(page) DO UPDATE SET
            listing_url=excluded.listing_url,
            scanned_at=excluded.scanned_at
    """, (page, listing_url, now_utc_iso()))
    conn.commit()


def build_page_url(base_url: str, page: int) -> list[str]:
    if page == 1:
        return [base_url]
    base = base_url if base_url.endswith("/") else base_url + "/"
    return [
        urljoin(base, f"{page}/"),
        urljoin(base, f"{page}"),
        base.rstrip("/") + f"/?page={page}",
        base.rstrip("/") + f"/?p={page}",
        base.rstrip("/") + f"/?oldal={page}",
    ]


def pick_best_links(listing_url: str, row: BeautifulSoup):
    # Collect all non-asset links in the row
    links = []
    for a in row.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        if "/assets/" in href:
            continue
        links.append(urljoin(listing_url, href))

    producer_url = next((u for u in links if "/eloado/" in u), None)

    # Episode/detail urls tend to contain /adas/ or other non-listing patterns
    def is_episode_candidate(u: str) -> bool:
        if "/adasok/uj" in u:
            return False
        if "/adasok/datum/" in u:
            return False
        if "/adasok/nepszeru" in u:
            return False
        # candidate patterns
        return ("/adas/" in u) or ("/adasok/" in u and "/adasok/" not in ("/adasok/uj", "/adasok/datum/"))

    episode_url = next((u for u in links if is_episode_candidate(u)), None)

    # If no href episode link, try onclick
    if not episode_url:
        onclick = row.get("onclick", "") or ""
        m = re.search(r"(https?://[^\s'\";]+|/[A-Za-z0-9_\-./%]+)", onclick)
        if m:
            episode_url = urljoin(listing_url, m.group(1))

    return episode_url, producer_url


def extract_episode_entries_from_listing(html: str, listing_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    entries = []
    seen = set()

    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue

        title = cells[0].get_text(" ", strip=True)
        producer = cells[1].get_text(" ", strip=True)

        row_text = row.get_text(" ", strip=True)
        date = ""
        dm = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", row_text)
        if dm:
            date = dm.group(1)

        episode_url, producer_url = pick_best_links(listing_url, row)

        # IMPORTANT: we only insert if we have at least one useful URL
        key = episode_url or producer_url
        if not key:
            continue

        if key in seen:
            continue
        seen.add(key)

        entries.append({
            "episode_url": episode_url or producer_url,  # primary key
            "producer_url": producer_url,
            "title": title,
            "producer": producer,
            "date": date,
        })

    return entries


def upsert_episode(conn, ep: dict):
    conn.execute("""
        INSERT INTO episodes (episode_url, producer_url, title, producer, date, audio_url, status, filename, error, added_at, downloaded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(episode_url) DO UPDATE SET
            producer_url=COALESCE(excluded.producer_url, producer_url),
            title=COALESCE(excluded.title, title),
            producer=COALESCE(excluded.producer, producer),
            date=COALESCE(excluded.date, date)
    """, (
        ep["episode_url"],
        ep.get("producer_url"),
        ep.get("title"),
        ep.get("producer"),
        ep.get("date"),
        None,
        "queued",
        None,
        None,
        now_utc_iso(),
        None
    ))
    conn.commit()


def mark_status(conn, episode_url: str, status: str, audio_url=None, filename=None, error=None):
    downloaded_at = now_utc_iso() if status == "downloaded" else None
    conn.execute("""
        UPDATE episodes
        SET status=?,
            audio_url=COALESCE(?, audio_url),
            filename=COALESCE(?, filename),
            error=COALESCE(?, error),
            downloaded_at=COALESCE(?, downloaded_at)
        WHERE episode_url=?
    """, (status, audio_url, filename, error, downloaded_at, episode_url))
    conn.commit()


def extract_audio_urls_from_html(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls = set()

    # audio tags
    for audio in soup.find_all("audio"):
        src = audio.get("src")
        if src:
            urls.add(urljoin(base_url, src))
        for source in audio.find_all("source"):
            s = source.get("src")
            if s:
                urls.add(urljoin(base_url, s))

    for source in soup.find_all("source"):
        s = source.get("src")
        if s:
            urls.add(urljoin(base_url, s))

    # direct extension links
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        absu = urljoin(base_url, href)
        if any(absu.lower().split("?")[0].endswith(ext) for ext in AUDIO_EXTS):
            urls.add(absu)

    # regex absolute urls
    for m in re.finditer(r"https?://[^\s'\"<>]+", html):
        u = m.group(0)
        base = u.lower().split("?")[0]
        if any(base.endswith(ext) for ext in AUDIO_EXTS):
            urls.add(u)

    return sorted(urls)


def extract_rss_url_from_producer_page(html: str) -> str | None:
    # Often displayed like: "RSS: https://...."
    m = re.search(r"RSS:\s*(https?://\S+)", html)
    if m:
        return m.group(1).strip().rstrip(")\"'")

    soup = BeautifulSoup(html, "html.parser")
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        text = a.get_text(" ", strip=True).lower()
        if "rss" in text and href.startswith("http"):
            return href

    return None


def norm_title(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return s


def find_enclosure_by_title(rss_url: str, wanted_title: str) -> str | None:
    feed = feedparser.parse(rss_url)
    if not feed or not getattr(feed, "entries", None):
        return None

    wt = norm_title(wanted_title)
    best = None

    for e in feed.entries:
        et = norm_title(getattr(e, "title", "") or "")
        if not et:
            continue

        # title match (loose)
        if wt and (wt == et or wt in et or et in wt):
            # enclosure
            enclosures = getattr(e, "enclosures", None) or []
            if enclosures:
                href = enclosures[0].get("href")
                if href:
                    return href

            # some feeds store it as links
            for l in getattr(e, "links", []) or []:
                if l.get("rel") == "enclosure" and l.get("href"):
                    return l["href"]

        # fallback best guess (store first enclosure found)
        if best is None:
            enclosures = getattr(e, "enclosures", None) or []
            if enclosures and enclosures[0].get("href"):
                best = enclosures[0]["href"]

    return best


def stream_download(session: requests.Session, url: str, out_path: Path, timeout: int = 60):
    with session.get(url, stream=True, timeout=timeout, allow_redirects=True) as r:
        r.raise_for_status()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = out_path.with_suffix(out_path.suffix + ".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
        tmp.replace(out_path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL)
    ap.add_argument("--out", default="./podcasts")
    ap.add_argument("--db", default="podkaszt.sqlite")
    ap.add_argument("--max-pages", type=int, default=1)
    ap.add_argument("--start-page", type=int, default=1)
    ap.add_argument("--delay", type=float, default=0.7)
    ap.add_argument("--collect-only", action="store_true")
    ap.add_argument("--force-rescan", action="store_true")
    args = ap.parse_args()

    base_url = args.base_url if args.base_url.endswith("/") else (args.base_url + "/")
    out_dir = Path(args.out)
    db_path = Path(args.db)

    session = make_session()
    conn = init_db(db_path)

    print(f"[i] Base: {base_url}")
    print(f"[i] DB: {db_path.resolve()}")
    print(f"[i] Out: {out_dir.resolve()}")

    last_scanned = meta_get(conn, "last_listing_page_scanned")
    last_scanned_int = int(last_scanned) if last_scanned and last_scanned.isdigit() else 0
    start_page = max(args.start_page, last_scanned_int + 1) if not args.force_rescan else args.start_page

    if not args.force_rescan and last_scanned_int > 0:
        print(f"[i] Resume listing scan from page {start_page} (last scanned: {last_scanned_int})")

    # 1) Scan listing pages
    for page in range(start_page, args.max_pages + 1):
        if (not args.force_rescan) and is_page_scanned(conn, page):
            continue

        urls_to_try = build_page_url(base_url, page)
        listing_html = None
        listing_url_used = None
        last_err = None

        for u in urls_to_try:
            try:
                listing_html = fetch_html(session, u)
                listing_url_used = u
                break
            except Exception as e:
                last_err = str(e)

        if not listing_html:
            print(f"[!] Page {page}: failed to fetch. Error: {last_err}")
            return

        entries = extract_episode_entries_from_listing(listing_html, listing_url_used)
        print(f"[i] Page {page}: found {len(entries)} entries")

        for ep in entries:
            upsert_episode(conn, ep)

        mark_page_scanned(conn, page, listing_url_used)
        meta_set(conn, "last_listing_page_scanned", str(page))
        polite_sleep(args.delay)

    if args.collect_only:
        print("[i] collect-only finished.")
        return

    # 2) Download
    rows = conn.execute("""
        SELECT episode_url, producer_url, title, producer
        FROM episodes
        WHERE status IS NULL OR status NOT IN ('downloaded')
        ORDER BY added_at ASC
    """).fetchall()

    print(f"[i] Pending downloads: {len(rows)}")

    for (episode_url, producer_url, title, producer) in rows:
        try:
            html = fetch_html(session, episode_url)
            polite_sleep(args.delay)

            audio_urls = extract_audio_urls_from_html(html, episode_url)
            audio_url = audio_urls[0] if audio_urls else None

            # RSS fallback if no audio found
            if not audio_url and producer_url:
                # cache rss url
                rowf = conn.execute("SELECT rss_url FROM feeds WHERE producer_url=?", (producer_url,)).fetchone()
                rss_url = rowf[0] if rowf else None

                if not rss_url:
                    prod_html = fetch_html(session, producer_url)
                    polite_sleep(args.delay)
                    rss_url = extract_rss_url_from_producer_page(prod_html)
                    conn.execute("""
                        INSERT INTO feeds(producer_url, rss_url, status, error, updated_at)
                        VALUES(?, ?, ?, ?, ?)
                        ON CONFLICT(producer_url) DO UPDATE SET
                            rss_url=excluded.rss_url,
                            status=excluded.status,
                            error=excluded.error,
                            updated_at=excluded.updated_at
                    """, (producer_url, rss_url, "ok" if rss_url else "no_rss", None, now_utc_iso()))
                    conn.commit()

                if rss_url:
                    audio_url = find_enclosure_by_title(rss_url, title)

            if not audio_url:
                mark_status(conn, episode_url, "no_audio", error="No audio URL found (HTML+RSS).")
                print(f"[!] no_audio: {episode_url}  (title='{title}')")
                continue

            parsed = urlparse(audio_url)
            ext = os.path.splitext(parsed.path)[1].lower()
            if ext not in AUDIO_EXTS:
                ext = ".mp3"

            base_name = slugify(f"{title} - {producer}".strip(" -"))
            unique = sha1(episode_url)[:10]
            filename = f"{base_name} [{unique}]{ext}"
            out_path = out_dir / filename

            if out_path.exists() and out_path.stat().st_size > 0:
                mark_status(conn, episode_url, "downloaded", audio_url=audio_url, filename=str(out_path))
                print(f"[i] already: {out_path.name}")
                continue

            print(f"[+] download: {out_path.name}")
            stream_download(session, audio_url, out_path)
            mark_status(conn, episode_url, "downloaded", audio_url=audio_url, filename=str(out_path))

            polite_sleep(args.delay)

        except Exception as e:
            mark_status(conn, episode_url, "error", error=str(e))
            print(f"[!] error: {episode_url} -> {e}")

    print("[i] Done.")


if __name__ == "__main__":
    main()
