#!/usr/bin/env python3
import argparse
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

BASE = "https://soundcloud.com"


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_name(name: str, max_len: int = 80) -> str:
    name = name.strip()
    name = re.sub(r"\s+", " ", name)
    name = re.sub(r"[^\w\-\. ]+", "_", name, flags=re.UNICODE)
    name = name.replace(" ", "_")
    return name[:max_len] if len(name) > max_len else name


def strip_query(url: str) -> str:
    """Remove query/fragment to make stable keys for visited/library."""
    p = urlparse(url)
    return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))


@dataclass
class PlaylistData:
    url: str
    title: str
    tracks: List[str]


class SoundCloudCollector:
    def __init__(self, out_dir: Path, sleep_s: float = 0.5):
        self.out_dir = out_dir
        self.sleep_s = sleep_s
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; sc-collector/1.0; +https://soundcloud.com)",
            "Accept-Language": "en-US,en;q=0.9,hu;q=0.8",
        })

        self.library_path = self.out_dir / "library.json"
        self.visited_path = self.out_dir / "visited.json"
        self.playlists_dir = self.out_dir / "playlists"

        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.playlists_dir.mkdir(parents=True, exist_ok=True)

    # ---------- persistence ----------
    def load_json(self, path: Path, default):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        return default

    def save_json(self, path: Path, obj) -> None:
        path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

    def load_library(self) -> Dict[str, dict]:
        return self.load_json(self.library_path, {})

    def load_visited(self) -> Dict[str, dict]:
        return self.load_json(self.visited_path, {})

    # ---------- fetch & parse ----------
    def fetch_html(self, url: str) -> str:
        r = self.session.get(url, timeout=30)
        r.raise_for_status()
        time.sleep(self.sleep_s)
        return r.text

    def extract_playlist_urls_from_search(self, search_url: str) -> List[str]:
        """
        Extract playlist URLs from the 'no-JS' HTML version of the search page.
        Note: This may not return ALL results compared to JS infinite scroll.
        """
        html = self.fetch_html(search_url)
        soup = BeautifulSoup(html, "html.parser")

        urls: Set[str] = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if "/sets/" in href:
                full = urljoin(BASE, href)
                urls.add(strip_query(full))

        return sorted(urls)

    def extract_playlist_title_and_tracks(self, playlist_url: str) -> PlaylistData:
        html = self.fetch_html(playlist_url)
        soup = BeautifulSoup(html, "html.parser")

        # Title: first H1 is usually "Playlist name by User"
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else playlist_url

        # Track links: typically /{user}/{track-slug} (2 path segments)
        track_urls: Set[str] = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href.startswith("/"):
                continue
            if "/sets/" in href:
                continue  # skip playlist links
            # ignore obvious non-track endpoints
            parts = href.strip("/").split("/")
            if len(parts) != 2:
                continue
            if parts[1] in {"tracks", "albums", "sets", "reposts", "likes", "followers", "following"}:
                continue

            full = urljoin(BASE, href)
            track_urls.add(strip_query(full))

        return PlaylistData(url=strip_query(playlist_url), title=title, tracks=sorted(track_urls))

    # ---------- output structure ----------
    def write_playlist_folder(self, pl: PlaylistData) -> None:
        folder = self.playlists_dir / sanitize_name(pl.title)
        folder.mkdir(parents=True, exist_ok=True)

        (folder / "playlist_url.txt").write_text(pl.url + "\n", encoding="utf-8")
        (folder / "tracks.txt").write_text("\n".join(pl.tracks) + ("\n" if pl.tracks else ""), encoding="utf-8")

        meta = {
            "url": pl.url,
            "title": pl.title,
            "track_count": len(pl.tracks),
            "updated_at": iso_now(),
            "tracks": pl.tracks,
        }
        self.save_json(folder / "playlist.json", meta)

    # ---------- commands ----------
    def cmd_collect(self, search_url: str, limit_playlists: Optional[int] = None) -> None:
        library = self.load_library()

        playlist_urls = self.extract_playlist_urls_from_search(search_url)
        if limit_playlists is not None:
            playlist_urls = playlist_urls[:limit_playlists]

        print(f"Found {len(playlist_urls)} playlist URLs on search page.")
        for i, pl_url in enumerate(playlist_urls, 1):
            try:
                pl = self.extract_playlist_title_and_tracks(pl_url)
                self.write_playlist_folder(pl)

                library[pl.url] = {
                    "title": pl.title,
                    "tracks": pl.tracks,
                    "track_count": len(pl.tracks),
                    "updated_at": iso_now(),
                    "source_search_url": strip_query(search_url),
                }
                print(f"[{i}/{len(playlist_urls)}] {pl.title} -> {len(pl.tracks)} tracks")
            except requests.HTTPError as e:
                print(f"[{i}/{len(playlist_urls)}] ERROR fetching {pl_url}: {e}")

        self.save_json(self.library_path, library)
        print(f"Library saved: {self.library_path}")

    def cmd_next(self, n: int) -> None:
        library = self.load_library()
        visited = self.load_visited()

        remaining: List[Tuple[str, str]] = []  # (playlist_title, track_url)

        for pl_url, pl_meta in library.items():
            title = pl_meta.get("title", pl_url)
            for t in pl_meta.get("tracks", []):
                key = strip_query(t)
                if key not in visited:
                    remaining.append((title, key))

        print(f"Unvisited tracks total: {len(remaining)}")
        for title, url in remaining[:n]:
            print(f"{title} :: {url}")

    def cmd_mark(self, urls: List[str], note: str = "") -> None:
        visited = self.load_visited()

        for u in urls:
            key = strip_query(u)
            visited[key] = {
                "visited_at": iso_now(),
                "note": note,
            }
            print(f"Marked visited: {key}")

        self.save_json(self.visited_path, visited)
        print(f"Visited saved: {self.visited_path}")


def main():
    p = argparse.ArgumentParser(description="SoundCloud playlist/track link collector + visited list.")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_collect = sub.add_parser("collect", help="Collect playlists + tracks from a SoundCloud search/sets URL.")
    p_collect.add_argument("--url", required=True, help="SoundCloud search/sets URL")
    p_collect.add_argument("--out", required=True, help="Output folder")
    p_collect.add_argument("--limit-playlists", type=int, default=None, help="Optional limit for number of playlists")

    p_next = sub.add_parser("next", help="Print next unvisited tracks (links).")
    p_next.add_argument("--out", required=True, help="Output folder")
    p_next.add_argument("--n", type=int, default=20, help="How many to print")

    p_mark = sub.add_parser("mark", help="Mark one or more track URLs as visited.")
    p_mark.add_argument("--out", required=True, help="Output folder")
    p_mark.add_argument("--url", action="append", required=True, help="Track URL (can be given multiple times)")
    p_mark.add_argument("--note", default="", help="Optional note (e.g., processed by pipeline X)")

    args = p.parse_args()

    out_dir = Path(args.out).expanduser().resolve()
    sc = SoundCloudCollector(out_dir)

    if args.cmd == "collect":
        sc.cmd_collect(args.url, limit_playlists=args.limit_playlists)
    elif args.cmd == "next":
        sc.cmd_next(args.n)
    elif args.cmd == "mark":
        sc.cmd_mark(args.url, note=args.note)


if __name__ == "__main__":
    main()
