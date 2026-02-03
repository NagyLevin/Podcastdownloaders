#!/usr/bin/env python3
import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import feedparser
import requests


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_name(s: str, max_len: int = 120) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w\-. ]+", "_", s, flags=re.UNICODE)
    s = s.replace(" ", "_")
    return s[:max_len] if len(s) > max_len else s


def load_json(path: Path, default):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return default


def save_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_feeds_file(path: Path) -> List[Tuple[Optional[str], str]]:
    feeds: List[Tuple[Optional[str], str]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "|" in line:
            name, url = line.split("|", 1)
            feeds.append((name.strip() or None, url.strip()))
        else:
            feeds.append((None, line))
    return feeds


def best_enclosure(entry) -> Optional[str]:
    # Standard RSS: entry.enclosures[0].href
    enclosures = getattr(entry, "enclosures", None) or []
    for enc in enclosures:
        href = getattr(enc, "href", None) or enc.get("href")
        if href:
            return href

    # Sometimes: entry.links with rel="enclosure"
    links = getattr(entry, "links", None) or []
    for l in links:
        if (l.get("rel") == "enclosure") and l.get("href"):
            return l["href"]

    return None


def stable_episode_key(feed_url: str, entry) -> str:
    # Prefer GUID/ID, else fallback to enclosure URL, else link/title combo
    guid = getattr(entry, "id", None) or entry.get("id")
    if guid:
        return f"{feed_url}::id::{guid}"

    enc = best_enclosure(entry)
    if enc:
        return f"{feed_url}::enc::{enc}"

    link = getattr(entry, "link", None) or entry.get("link") or ""
    title = getattr(entry, "title", None) or entry.get("title") or ""
    return f"{feed_url}::lt::{link}::{title}"


def download_file(url: str, dest: Path, timeout_s: int = 60) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=timeout_s) as r:
        r.raise_for_status()
        tmp = dest.with_suffix(dest.suffix + ".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
        tmp.replace(dest)


def pick_extension_from_url(url: str) -> str:
    # very simple heuristic
    m = re.search(r"\.([a-zA-Z0-9]{2,5})(?:\?|$)", url)
    if m:
        ext = m.group(1).lower()
        if ext in {"mp3", "m4a", "aac", "wav", "ogg", "opus", "flac"}:
            return "." + ext
    return ".mp3"


def main():
    ap = argparse.ArgumentParser(description="RSS podcast downloader with visited.json (no re-download).")
    ap.add_argument("--feeds", required=True, help="Path to feeds.txt (one RSS per line, optional Name|URL).")
    ap.add_argument("--out", required=True, help="Output directory.")
    ap.add_argument("--limit", type=int, default=0, help="Max episodes per feed this run (0 = no limit).")
    ap.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds.")
    ap.add_argument("--dry-run", action="store_true", help="Parse feeds and list what would download, without downloading.")
    args = ap.parse_args()

    out_dir = Path(args.out).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    visited_path = out_dir / "visited.json"
    visited: Dict[str, dict] = load_json(visited_path, {})

    feeds = parse_feeds_file(Path(args.feeds))
    print(f"Feeds: {len(feeds)} | Output: {out_dir}")

    session = requests.Session()
    session.headers.update({"User-Agent": "podcast-rss-downloader/1.0"})

    total_new = 0

    for custom_name, feed_url in feeds:
        print(f"\n== Feed: {feed_url}")
        d = feedparser.parse(feed_url)

        feed_title = (d.feed.get("title") if hasattr(d, "feed") else None) or custom_name or feed_url
        folder_name = sanitize_name(custom_name or feed_title)
        feed_dir = out_dir / folder_name
        feed_dir.mkdir(parents=True, exist_ok=True)

        # save feed metadata snapshot
        save_json(feed_dir / "feed_meta.json", {
            "feed_url": feed_url,
            "title": feed_title,
            "updated_at": iso_now(),
        })

        entries = list(getattr(d, "entries", []) or [])
        print(f"Entries found: {len(entries)}")

        downloaded_this_feed = 0
        for entry in entries:
            if args.limit and downloaded_this_feed >= args.limit:
                break

            enc = best_enclosure(entry)
            if not enc:
                continue

            key = stable_episode_key(feed_url, entry)
            if key in visited:
                continue

            title = getattr(entry, "title", None) or entry.get("title") or "episode"
            safe_title = sanitize_name(title, max_len=140)

            ext = pick_extension_from_url(enc)
            filename = f"{safe_title}{ext}"
            dest = feed_dir / filename

            # avoid overwriting if same filename exists
            if dest.exists():
                # add a short suffix
                suffix = sanitize_name(str(abs(hash(key)))[:8])
                dest = feed_dir / f"{safe_title}_{suffix}{ext}"

            print(f"NEW: {title}")
            print(f"  -> {dest.name}")
            if not args.dry_run:
                # use requests directly (session.get stream)
                with session.get(enc, stream=True, timeout=args.timeout) as r:
                    r.raise_for_status()
                    tmp = dest.with_suffix(dest.suffix + ".part")
                    with open(tmp, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1024 * 256):
                            if chunk:
                                f.write(chunk)
                    tmp.replace(dest)

            visited[key] = {
                "visited_at": iso_now(),
                "feed_url": feed_url,
                "feed_title": feed_title,
                "episode_title": title,
                "enclosure_url": enc,
                "saved_as": str(dest.relative_to(out_dir)),
            }

            downloaded_this_feed += 1
            total_new += 1

        print(f"Downloaded new this feed: {downloaded_this_feed}")

    save_json(visited_path, visited)
    print(f"\nDone. New downloads: {total_new}")
    print(f"Visited saved: {visited_path}")


if __name__ == "__main__":
    main()
