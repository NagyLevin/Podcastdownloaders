import argparse
import hashlib
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict
from urllib.parse import urlparse, urlsplit, urlunsplit, unquote

import requests
import requests.exceptions as req_exc
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from playwright.sync_api import sync_playwright, TimeoutError as PwTimeoutError

BASE_URL_DEFAULT = "https://podkaszt.hu/adasok/uj/"  # podcastok 20 oldalanként 3500 oldal
AUDIO_EXTS = (".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav")
TMP_SUFFIX = ".part"

COOKIE_ACCEPT_REGEX = re.compile(r"(Beleegyezés|Elfogadom|Accept|Agree|Allow)", re.I)


# ----------------- idő / formázók -----------------

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def local_ts() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")


def fmt_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def fmt_bytes(n: float) -> str:
    n = float(n)
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while n >= 1024.0 and i < len(units) - 1:
        n /= 1024.0
        i += 1
    if i == 0:
        return f"{int(n)}{units[i]}"
    return f"{n:.1f}{units[i]}"


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


# ----------------- fájlnév tisztítás -----------------

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


# ----------------- visited / timeout log -----------------

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


def append_dead(path: Path, url: str, reason: str):
    """
    Opcionális: ide rakjuk a nagy eséllyel „végleges” hibákat (404, DNS, stb),
    hogy később ne pörögj rájuk újra feleslegesen.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    reason = (reason or "").replace("\n", " ").replace("\r", " ").strip()
    line = f"{now_utc_iso()}\t{url}\t{reason}\n"
    with open(path, "a", encoding="utf-8") as f:
        f.write(line)


# ----------------- Playwright segéd -----------------

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


def wait_for_overlays(page, timeout_ms: int = 4000):
    """
    A timeouts.txt-ben látszott "spinner intercepts pointer events" jellegű kattintási hiba.
    Itt több tipikus overlay/spinner szelektort próbálunk eltüntetni.
    (Nem baj, ha nincs ilyen elem.)
    """
    selectors = [
        "#spinner",
        ".spinner",
        ".loading",
        ".overlay",
        ".modal-backdrop",
        "[aria-busy='true']",
        ".fa-spinner",
        "app-spinner",
    ]
    t0 = time.monotonic()
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                # várjuk, hogy eltűnjön / ne blokkoljon
                loc.first.wait_for(state="hidden", timeout=max(500, timeout_ms // 2))
        except Exception:
            pass
        if (time.monotonic() - t0) * 1000 > timeout_ms:
            break


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
    """
    Először megpróbáljuk a <audio>/<video> currentSrc/src értékét.
    Ha blob: URL, azt elengedjük (az nem letölthető egyszerűen).
    """
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


# ----------------- URL és host-specifikus fixek -----------------

def guess_ext_from_url(url: str) -> str:
    path = urlparse(url).path
    ext = os.path.splitext(path)[1].lower()
    return ext if ext in AUDIO_EXTS else ".mp3"


def sanitize_audio_url(url: str) -> str:
    """
    timeouts.txt-ben volt olyan, hogy host='http' (pl. https://http/feeds.soundcloud.com/...)
    illetve 'https://http://...' jellegű elcsúszás.
    Itt ezeket kigyomláljuk.
    """
    url = (url or "").strip()

    # 1) https://http/...
    if url.startswith("https://http/"):
        url = "https://" + url[len("https://http/"):]
    if url.startswith("http://http/"):
        url = "http://" + url[len("http://http/"):]

    # 2) https://http://...
    if url.startswith("https://http://"):
        url = "http://" + url[len("https://http://"):]
    if url.startswith("http://https://"):
        url = "https://" + url[len("http://https://"):]

    # 3) ha nincs scheme
    p = urlparse(url)
    if not p.scheme:
        url = "https://" + url.lstrip("/")

    return url


def rewrite_special_hosts(url: str) -> str:
    """
    - g7.p3k.hu -> g7.hu (gyakori routing 'no route to host' / connect timeout)
    """
    try:
        parts = urlsplit(url)
        netloc = parts.netloc.lower()

        if netloc == "g7.p3k.hu":
            new_parts = (parts.scheme, "g7.hu", parts.path, parts.query, parts.fragment)
            return urlunsplit(new_parts)

    except Exception:
        pass
    return url


def decode_anchor_wrapped_url(url: str) -> str:
    """
    Anchor.fm-nél gyakori minta:
      https://anchor.fm/.../podcast/play/.../http%3A%2F%2Fexample.com%2Ffile.mp3
    Itt az utolsó %-enkódolt rész az igazi média URL. Dekódoljuk.
    """
    try:
        parts = urlsplit(url)
        if "anchor.fm" not in parts.netloc.lower():
            return url
        if "/podcast/play/" not in parts.path:
            return url

        # Keressünk egy "http%3A%2F%2F" vagy "https%3A%2F%2F" szegmenst
        if "http%3a%2f%2f" not in url.lower() and "https%3a%2f%2f" not in url.lower():
            return url

        last_seg = parts.path.split("/")[-1]
        dec = unquote(last_seg)
        if dec.startswith("http://") or dec.startswith("https://"):
            return dec

    except Exception:
        pass
    return url


def https_to_http_fallback(url: str) -> str:
    """
    podcasts.faklyaradio.hu esetén a timeouts.txt-ben nagyon sok "connection refused" volt https-en.
    Itt adunk egy lehetőséget: https -> http próbálkozás.
    """
    try:
        parts = urlsplit(url)
        if parts.scheme == "https" and parts.netloc.lower() == "podcasts.faklyaradio.hu":
            return urlunsplit(("http", parts.netloc, parts.path, parts.query, parts.fragment))
    except Exception:
        pass
    return url


def normalize_media_url(url: str) -> str:
    """
    Egyetlen helyre összefogjuk:
    - sanitize (host='http' jelleg)
    - anchor dekódolás
    - g7 host rewrite
    """
    url = sanitize_audio_url(url)
    url = decode_anchor_wrapped_url(url)
    url = rewrite_special_hosts(url)
    return url


# ----------------- requests session + cookie sync -----------------

def build_http_session(http_retries: int, backoff: float) -> requests.Session:
    """
    requests.Session Retry-vel:
    - főleg a connect / initial handshake problémákat fogja meg
    - mid-stream megszakadásnál a saját (download_with_resume) retry a lényeg
    """
    s = requests.Session()

    retry = Retry(
        total=http_retries,
        connect=http_retries,
        read=http_retries,
        status=http_retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504, 522],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def sync_cookies_to_requests(context, session: requests.Session):
    jar = requests.cookies.RequestsCookieJar()
    for c in context.cookies():
        jar.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path", "/"))
    session.cookies = jar


# ----------------- HEADERS / HOST RATE LIMIT -----------------

def browserish_headers(referer: str) -> Dict[str, str]:
    """
    Általános „böngészős” header csomag.
    406/403/WAF esetén gyakran segít, hogy ne nézzen ki botnak.
    """
    return {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/121.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Accept-Language": "hu-HU,hu;q=0.9,en-US;q=0.7,en;q=0.6",
        "Referer": referer,
        "Connection": "keep-alive",
    }


def headers_for_url(url: str, referer: str) -> Dict[str, str]:
    """
    Host-specifikus header finomhangolás:
    - archive.tilos.hu (406) -> különösen fontos a böngésző-szerű Accept/Accept-Language
    """
    h = browserish_headers(referer)
    host = ""
    try:
        host = urlsplit(url).netloc.lower()
    except Exception:
        host = ""

    # Tilosnál néha a *//* accept is kell, máskor jobb konkrétabb:
    if host == "archive.tilos.hu":
        h["Accept"] = "audio/*,*/*;q=0.8"
    return h


def host_key(url: str) -> str:
    try:
        return urlsplit(url).netloc.lower()
    except Exception:
        return ""


def ensure_host_delay(url: str, host_last_ts: Dict[str, float], default_delay: float, g7_delay: float, tilos_delay: float):
    """
    Egyszerű rate-limit: hostonként tartunk utolsó request időpontot.
    A g7 és tilos jobban érzékeny (522/406), ott nagyobb delay-t használunk.
    """
    h = host_key(url)
    delay = default_delay
    if h in ("g7.hu", "g7.p3k.hu"):
        delay = max(delay, g7_delay)
    if h == "archive.tilos.hu":
        delay = max(delay, tilos_delay)

    now = time.monotonic()
    prev = host_last_ts.get(h, 0.0)
    wait = (prev + delay) - now
    if wait > 0:
        time.sleep(wait)
    host_last_ts[h] = time.monotonic()


# ----------------- letöltés / resume / retry -----------------

def get_total_size_bytes(existing: int, r: requests.Response) -> Optional[int]:
    """
    Visszaadja a teljes fájlméretet byte-ban, ha kiszedhető a headerből.
    Range-es letöltésnél Content-Range a legjobb (bytes a-b/TOTAL).
    """
    cr = r.headers.get("Content-Range")
    if cr:
        # pl: "bytes 123-999/1000"
        m = re.search(r"/(\d+)\s*$", cr)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass

    cl = r.headers.get("Content-Length")
    if cl:
        try:
            clen = int(cl)
            # Ha 206 (partial content), a Content-Length a "maradék", tehát total = existing + remaining
            if r.status_code == 206:
                return existing + clen
            return clen
        except Exception:
            pass

    return None


def is_permanent_http(status: int) -> bool:
    """
    Egyszerű heuristika:
    - 404/410: nagyon gyakran végleges (törölt)
    - 401/403/406: lehet WAF / header-probléma (nem mindig végleges)
      -> itt nem tekintjük automatikusan véglegesnek, mert host-specifikus javítás segíthet
    """
    return status in (404, 410)


def download_with_resume(
    session: requests.Session,
    url: str,
    out_path: Path,
    referer: str,
    progress_label: str = "",
    connect_timeout: float = 20.0,
    read_timeout: float = 900.0,
    attempts: int = 8,
    backoff: float = 1.3,
    host_last_ts: Optional[Dict[str, float]] = None,
    default_delay: float = 0.4,
    g7_delay: float = 1.2,
    tilos_delay: float = 1.0,
    faklya_http_fallback_enabled: bool = True,
    dead_links_path: Optional[Path] = None,
):
    """
    Letöltés resume-mal + élő progress + erősített retry/backoff:
    - mid-stream megszakadásnál (ReadTimeout/ConnectionError/ChunkedEncodingError) is folytatja
    - 416 esetén törli a .part-ot és újrakezdi
    - 522/5xx esetén backoff + retry
    - (opcionálisan) végleges hibákat dead_links.txt-be gyűjti
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + TMP_SUFFIX)

    url0 = normalize_media_url(url)

    # Host-rate-limit támogatás (ha adtunk dictet)
    if host_last_ts is None:
        host_last_ts = {}

    last_err = None
    started_at_global = local_ts()

    for attempt in range(1, max(1, attempts) + 1):
        try:
            # A URL-t minden próbánál normalizáljuk (biztonság kedvéért)
            cur_url = normalize_media_url(url0)

            # Egyes hostoknál érdemes rate-limitelni
            ensure_host_delay(
                cur_url,
                host_last_ts=host_last_ts,
                default_delay=default_delay,
                g7_delay=g7_delay,
                tilos_delay=tilos_delay,
            )

            existing = tmp.stat().st_size if tmp.exists() else 0

            headers = headers_for_url(cur_url, referer=referer)
            if existing > 0:
                headers["Range"] = f"bytes={existing}-"

            # --- HTTP kérés ---
            r = session.get(
                cur_url,
                stream=True,
                timeout=(connect_timeout, read_timeout),  # külön connect/read timeout
                allow_redirects=True,
                headers=headers,
            )

            # 416: Range rossz (pl. a fájl megváltozott, vagy a .part nagyobb, mint a szerver oldali)
            if r.status_code == 416 and existing > 0:
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
                # újrapróbáljuk Range nélkül
                raise RuntimeError("range_416_reset")

            # Végleges 404/410: ne erőltessük sokáig (de logoljuk)
            if is_permanent_http(r.status_code):
                reason = f"HTTP_{r.status_code}"
                if dead_links_path is not None:
                    append_dead(dead_links_path, cur_url, reason)
                raise RuntimeError(reason)

            # 429 / 5xx / 522: ezek gyakran átmenetiek -> backoff
            if r.status_code in (429, 500, 502, 503, 504, 522):
                raise RuntimeError(f"retriable_http_{r.status_code}")

            # Itt dobjuk a többi 4xx/5xx esetén (pl. 403/406/400)
            r.raise_for_status()

            # --- fájl mód (resume csak akkor, ha 206-ot ad) ---
            mode = "ab" if (existing > 0 and r.status_code == 206) else "wb"
            if mode == "wb":
                existing = 0
                # ha újrakezdünk, a régi tmp-t kidobjuk
                if tmp.exists():
                    try:
                        tmp.unlink(missing_ok=True)
                    except Exception:
                        pass

            total = get_total_size_bytes(existing, r)  # lehet None
            downloaded = existing
            session_bytes = 0  # az aktuális attemptben letöltött byte

            last_print = 0.0
            label = (progress_label or out_path.name or "download").strip()

            # csak az első attemptnél írjuk ki a "download start" sort (kevésbé spam)
            if attempt == 1:
                print(f"[i] download start: {started_at_global} | {label}")

            t0 = time.monotonic()

            with open(tmp, mode) as f:
                for chunk in r.iter_content(chunk_size=1024 * 256):
                    if not chunk:
                        continue
                    f.write(chunk)

                    downloaded += len(chunk)
                    session_bytes += len(chunk)

                    now = time.monotonic()
                    elapsed = max(0.001, now - t0)
                    speed = session_bytes / elapsed  # B/s

                    # ne frissítsünk túl gyakran
                    if (now - last_print) < 0.25:
                        continue
                    last_print = now

                    if total and total > 0:
                        pct = (downloaded / total) * 100.0
                        remain = max(0, total - downloaded)
                        eta = (remain / speed) if speed > 1e-9 else 0.0
                        msg = (
                            f"\r{pct:6.2f}%  {fmt_bytes(downloaded)}/{fmt_bytes(total)}"
                            f"  {fmt_bytes(speed)}/s  ETA {fmt_duration(eta)}  | {label}"
                        )
                    else:
                        msg = f"\r{fmt_bytes(downloaded)}  {fmt_bytes(speed)}/s  | {label}"

                    print(msg, end="", flush=True)

            # siker: átmozgatjuk végleges helyre
            tmp.replace(out_path)

            finished_at = local_ts()
            total_elapsed = time.monotonic() - t0
            avg_speed = session_bytes / max(0.001, total_elapsed)

            # progress sor lezárása
            if total and total > 0:
                print(
                    f"\r100.00%  {fmt_bytes(downloaded)}/{fmt_bytes(total)}  "
                    f"{fmt_bytes(avg_speed)}/s  ETA 00:00  | {label}"
                )
            else:
                print()

            print(f"[i] download end:   {finished_at} | {label} | elapsed {fmt_duration(total_elapsed)}")
            return  # kész

        except RuntimeError as e:
            last_err = str(e)

            # "véglegesnek" tekintett hibáknál (404/410) ne próbálkozzunk tovább
            if last_err.startswith("HTTP_404") or last_err.startswith("HTTP_410"):
                raise

            # 416 reset: azonnali újrapróba (kisebb backoff)
            if last_err == "range_416_reset":
                if attempt < attempts:
                    time.sleep(min(2.0, backoff))
                    continue
                raise

            # Faklya fallback: ha engedélyezett és https-es volt
            if faklya_http_fallback_enabled:
                new_url = https_to_http_fallback(url0)
                if new_url != url0:
                    url0 = new_url  # a következő attempt már http-vel megy
                    # kis várakozás
                    time.sleep(min(2.5, backoff))
                    continue

        except (req_exc.ReadTimeout, req_exc.ConnectTimeout, req_exc.ConnectionError, req_exc.ChunkedEncodingError) as e:
            last_err = f"{type(e).__name__}: {e}"

            # Faklya fallback kapcsolat hibákra
            if faklya_http_fallback_enabled:
                new_url = https_to_http_fallback(url0)
                if new_url != url0:
                    url0 = new_url
                    time.sleep(min(2.5, backoff))
                    continue

        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"

        # --- retry/backoff, ha van még próbálkozás ---
        if attempt >= attempts:
            break

        sleep_s = min(90.0, backoff * (2 ** (attempt - 1)))
        print(f"\n[!] download retry {attempt}/{attempts} -> {last_err} | sleep {sleep_s:.1f}s")
        time.sleep(sleep_s)

    # ha idáig jutunk: kifogytunk a próbákból
    raise RuntimeError(last_err or "download_failed_unknown")


# --------- fájlnév-hossz kezelése (byte-alapon) ---------

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

    producer_s = slugify(producer, max_len=4000)
    title_s = slugify(title, max_len=8000)

    key10 = (episode_key or "")[:10]
    stem = f"{producer_s} - {title_s} - {date} [{key10}]"

    name_max = get_name_max(out_dir)
    tail_bytes = len((ext + TMP_SUFFIX).encode("utf-8", errors="ignore"))
    max_stem_bytes = max(1, name_max - tail_bytes)

    safe_stem = trim_utf8_to_bytes(stem, max_stem_bytes, suffix="___")
    filename = safe_stem + ext

    path_max = get_path_max(out_dir)
    full_path_bytes = len(str(out_dir / filename).encode("utf-8", errors="ignore"))
    full_tmp_path_bytes = len(str(out_dir / (filename + TMP_SUFFIX)).encode("utf-8", errors="ignore"))
    if full_path_bytes > path_max or full_tmp_path_bytes > path_max:
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


# ----------------- MAIN -----------------

def main():
    ap = argparse.ArgumentParser()

    # Oldalazás / böngészés
    ap.add_argument("--base-url", default=BASE_URL_DEFAULT, help="base url of the podcast website")
    ap.add_argument("--start-page", type=int, default=1, help="start page of the podcast website")
    ap.add_argument("--end-page", type=int, default=2, help="end page of the podcast website")
    ap.add_argument("--profile", default=".pw-profile", help="Persistent browser profile folder (stores cookie consent)")
    ap.add_argument("--headful", action="store_true", help="show the browser window for debugging")
    ap.add_argument("--slowmo", type=int, default=0, help="ms slow motion for debugging (e.g. 150)")

    # Epizód kiválasztás / audio URL
    ap.add_argument("--audio-wait", type=float, default=12.0, help="wait time for the audio url")
    ap.add_argument("--retries", type=int, default=3, help="How many times to attempt getting an audio URL per episode")

    # Letöltés / hálózat
    ap.add_argument("--connect-timeout", type=float, default=20.0, help="TCP connect timeout (seconds)")
    ap.add_argument("--read-timeout", type=float, default=900.0, help="read timeout (seconds) - nagy mp3-nál emeld")
    ap.add_argument("--download-attempts", type=int, default=8, help="download retry attempts per episode")
    ap.add_argument("--http-retries", type=int, default=2, help="requests adapter retry (initial errors/status)")
    ap.add_argument("--backoff", type=float, default=1.3, help="exponential backoff factor (seconds)")

    # Rate-limit / host delay (522/406 csökkentése)
    ap.add_argument("--default-delay", type=float, default=0.4, help="min delay between requests per host")
    ap.add_argument("--g7-delay", type=float, default=1.2, help="extra delay for g7 hosts")
    ap.add_argument("--tilos-delay", type=float, default=1.0, help="extra delay for archive.tilos.hu")

    # Output / naplók
    ap.add_argument("--out", default="podcasts", help="output directory for the podcast files")
    ap.add_argument("--visited", default="podkaszt_visited.txt", help="visited file for the podcast files")
    ap.add_argument("--timeouts", default="timeouts.txt", help="timeout/error log file")
    ap.add_argument("--dead-links", default="dead_links.txt", help="permanent-ish bad links (404/410)")

    # Speciális
    ap.add_argument("--no-faklya-http-fallback", action="store_true", help="disable https->http fallback for faklyaradio")

    args = ap.parse_args()

    run_start = datetime.now().astimezone()
    print(f"[i] RUN START: {run_start.strftime('%Y-%m-%d %H:%M:%S')}")

    base_url = args.base_url if args.base_url.endswith("/") else (args.base_url + "/")
    out_dir = Path(args.out)
    visited_path = Path(args.visited)
    profile_dir = Path(args.profile)
    timeouts_path = Path(args.timeouts)
    dead_links_path = Path(args.dead_links)

    out_dir.mkdir(parents=True, exist_ok=True)
    visited = load_visited(visited_path)

    print(f"[i] Base:    {base_url}")
    print(f"[i] Out:     {out_dir.resolve()}")
    print(f"[i] Visited: {visited_path.resolve()} (loaded {len(visited)})")
    print(f"[i] Profile: {profile_dir.resolve()}")

    # host rate-limit állapot (host -> last_time)
    host_last_ts: Dict[str, float] = {}

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

        # requests session retry-vel
        session = build_http_session(args.http_retries, args.backoff)

        # Open base page & accept cookies
        page.goto(base_url, wait_until="domcontentloaded")
        page.wait_for_timeout(900)
        if accept_cookies_if_present(page):
            print("[i] Cookie consent accepted.")

        # Navigate to start page
        current = 1
        if args.start_page > 1:
            print(f"[i] skipping to start-page {args.start_page} ...")

        while current < args.start_page:
            target = current + 1
            ok = click_page_number(page, target)
            if not ok:
                ok = try_goto_page_by_url(page, base_url, target)

            if not ok:
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

        # Process pages
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

                if episode_key in visited:
                    continue

                page_candidates += 1

                # Select by clicking title
                try:
                    wait_for_overlays(page)
                    tds.nth(ti).click(timeout=2500)
                except Exception as e:
                    reason = f"select failed: {e}"
                    print(f"[!] failed: {producer} | {title} | {date} -> {reason}")
                    filename_nf = build_safe_filename(out_dir, producer, title, date, episode_key, ".mp3")
                    append_timeout(timeouts_path, filename_nf, date, producer, title, episode_key, reason)
                    continue

                # Try to start playback by clicking the icon column
                try:
                    wait_for_overlays(page)
                    tds.nth(0).click(timeout=1500)
                except Exception:
                    pass

                page.wait_for_timeout(700)

                # Try to obtain audio URL (currentSrc + network), with retries
                audio_url = None
                last_audio_from_net["url"] = None

                for _attempt in range(1, max(1, args.retries) + 1):
                    audio_url = get_audio_url_from_player(page, wait_s=args.audio_wait)
                    if audio_url and not audio_url.startswith("blob:"):
                        break

                    # network fallback
                    if last_audio_from_net["url"]:
                        audio_url = last_audio_from_net["url"]
                        break

                    # nudge playback again
                    try:
                        wait_for_overlays(page)
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

                # Sync cookies before downloading
                try:
                    sync_cookies_to_requests(context, session)
                except Exception:
                    pass

                # URL normalizálás (host='http', anchor decode, g7 rewrite, stb.)
                audio_url = normalize_media_url(audio_url)

                ext = guess_ext_from_url(audio_url)

                # Biztonságos fájlnév
                filename = build_safe_filename(out_dir, producer, title, date, episode_key, ext)
                out_path = out_dir / filename

                # Ha már letöltöttük, skip
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

                # Fájlrendszer hiba esetén (filename too long) még rövidítünk párszor
                for _fs_try in range(6):
                    try:
                        print(f"[+] downloading: {out_path.name}")
                        download_with_resume(
                            session=session,
                            url=audio_url,
                            out_path=out_path,
                            referer=base_url,
                            progress_label=out_path.name,
                            connect_timeout=args.connect_timeout,
                            read_timeout=args.read_timeout,
                            attempts=args.download_attempts,
                            backoff=args.backoff,
                            host_last_ts=host_last_ts,
                            default_delay=args.default_delay,
                            g7_delay=args.g7_delay,
                            tilos_delay=args.tilos_delay,
                            faklya_http_fallback_enabled=(not args.no_faklya_http_fallback),
                            dead_links_path=dead_links_path,
                        )
                        visited.add(episode_key)
                        append_visited(visited_path, episode_key, date, producer, title)
                        downloaded_ok = True
                        break

                    except OSError as e:
                        # filename too long
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

            # next page
            if page_no < args.end_page:
                ok = click_page_number(page, page_no + 1)
                if not ok:
                    ok = try_goto_page_by_url(page, base_url, page_no + 1)
                if not ok:
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
                        raise RuntimeError(f"Could not click pagination number {page_no + 1}")
                page.wait_for_timeout(700)

        context.close()

    run_end = datetime.now().astimezone()
    elapsed_s = (run_end - run_start).total_seconds()
    print(f"[i] RUN END:   {run_end.strftime('%Y-%m-%d %H:%M:%S')} | elapsed {fmt_duration(elapsed_s)}")
    print("[i] Done.")


if __name__ == "__main__":
    main()
    #python podkaszt_hu.py --start-page 1 --end-page 50 --retries 6 --audio-wait 25 --connect-timeout 20 --read-timeout 1200 --download-attempts 8 --backoff 1.3 --g7-delay 1.5 --tilos-delay 1.0 --default-delay 0.4 --out ./podcasts