#!/usr/bin/env python3
# sub_bot.py — single-file YouTube subscriber pool
# Runtime: Python 3.10+
#
# Commands:
#   python3 sub_bot.py run              OAuth loop (GCP / Data API v3)
#   python3 sub_bot.py cookies          cookie loop (InnerTube / SAPISIDHASH, no GCP)
#   python3 sub_bot.py mint ID SECRET   mint a refresh token (local, opens browser)
#   python3 sub_bot.py resolve @Handle  handle/name → UC channel ID
#   python3 sub_bot.py refresh          force-refresh every access token in the OAuth pool
#   python3 sub_bot.py status           OAuth pool + cache + quota stats
#   python3 sub_bot.py cookie-status    cookie-mode account stats
#
# Deps:
#   pip install requests
#   pip install google-auth-oauthlib   # only needed for `mint`
#
# Env:
#   OAuth mode:
#     GCP_PROJECTS     proj1:CLIENT_ID:SECRET  (one per line for multiple)
#     TARGET_CHANNELS  UCxxxx,UCyyyy
#     TOKENS           refresh1:proj1,refresh2:proj1
#     PROXIES          one per line, host:port or scheme://user:pass@host:port
#     ACCOUNT_COOLDOWN seconds between attempts per account (default 900)
#     GLOBAL_DELAY     seconds between subscribe attempts (default 45)
#     DB_PATH          sqlite path (default state.db)
#   Cookie mode:
#     COOKIES          one cookie blob per account, separated by a line of "---"
#                      plus TARGET_CHANNELS / PROXIES / delays as above
#
# subscriptions.insert = 50 quota units. Default GCP daily cap = 10,000 units
# => 200 subscribes/project/day.

from __future__ import annotations

import hashlib
import logging
import os
import random
import signal
import sqlite3
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from itertools import cycle

import requests

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

TOKEN_URL = "https://oauth2.googleapis.com/token"
SUB_API = "https://www.googleapis.com/youtube/v3/subscriptions"
CHANNELS_API = "https://www.googleapis.com/youtube/v3/channels"
SEARCH_API = "https://www.googleapis.com/youtube/v3/search"
INNERTUBE_KEY = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"
INNERTUBE_SUB = "https://www.youtube.com/youtubei/v1/subscription/subscribe"
CLIENT_VERSION = "2.20240901.00.00"   # bump from devtools if InnerTube 400s on unsupported client

QUOTA_PER_SUB = 50
DEFAULT_DAILY_CAP = 10000
ACCESS_TOKEN_SAFETY = 60
PREWARM_WINDOW = 300
AUTH_RETRIES = 3

log = logging.getLogger("sub_bot")
STOP = False

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GcpProject:
    project_id: str
    client_id: str
    client_secret: str


@dataclass(frozen=True)
class Config:
    projects: list[GcpProject]
    target_channels: list[str]
    proxies: list[str]
    account_cooldown: int
    global_delay: int
    db_path: str
    tokens: str


def _lines(raw: str) -> list[str]:
    return [ln.strip() for ln in raw.strip().splitlines() if ln.strip() and not ln.strip().startswith("#")]


def load_config(require_projects: bool = True) -> Config:
    projects: list[GcpProject] = []
    for ln in _lines(os.environ.get("GCP_PROJECTS", "")):
        parts = ln.split(":")
        if len(parts) != 3:
            raise SystemExit(f"bad GCP_PROJECTS line: {ln!r}")
        projects.append(GcpProject(*parts))

    channels = [c.strip() for c in os.environ.get("TARGET_CHANNELS", "").split(",") if c.strip()]

    if require_projects and not projects:
        raise SystemExit("GCP_PROJECTS is empty")
    if not channels:
        raise SystemExit("TARGET_CHANNELS is empty")

    return Config(
        projects=projects,
        target_channels=channels,
        proxies=_lines(os.environ.get("PROXIES", "")),
        account_cooldown=int(os.environ.get("ACCOUNT_COOLDOWN", "900")),
        global_delay=int(os.environ.get("GLOBAL_DELAY", "45")),
        db_path=os.environ.get("DB_PATH", "state.db"),
        tokens=os.environ.get("TOKENS", ""),
    )


# ---------------------------------------------------------------------------
# STORE
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS tokens (
    refresh_token TEXT PRIMARY KEY,
    project_id    TEXT NOT NULL,
    last_used     REAL NOT NULL DEFAULT 0,
    fail_count    INTEGER NOT NULL DEFAULT 0,
    dead          INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS access_tokens (
    refresh_token TEXT PRIMARY KEY,
    access_token  TEXT NOT NULL,
    expires_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS subs (
    refresh_token TEXT NOT NULL,
    channel_id    TEXT NOT NULL,
    status        TEXT NOT NULL,
    ts            REAL NOT NULL,
    PRIMARY KEY (refresh_token, channel_id)
);
CREATE TABLE IF NOT EXISTS project_quota (
    project_id  TEXT PRIMARY KEY,
    day         TEXT NOT NULL,
    used_units  INTEGER NOT NULL DEFAULT 0
);
"""


class Store:
    def __init__(self, path: str):
        self.path = path
        with self._conn() as c:
            c.executescript(SCHEMA)

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def add_token(self, refresh_token: str, project_id: str) -> None:
        with self._conn() as c:
            c.execute(
                "INSERT OR IGNORE INTO tokens (refresh_token, project_id) VALUES (?, ?)",
                (refresh_token, project_id),
            )

    def available_tokens(self, cooldown: int) -> list[sqlite3.Row]:
        cutoff = time.time() - cooldown
        with self._conn() as c:
            return list(c.execute(
                "SELECT * FROM tokens WHERE dead = 0 AND last_used < ? ORDER BY last_used ASC",
                (cutoff,),
            ).fetchall())

    def all_alive_tokens(self) -> list[sqlite3.Row]:
        with self._conn() as c:
            return list(c.execute("SELECT * FROM tokens WHERE dead = 0").fetchall())

    def mark_used(self, t: str) -> None:
        with self._conn() as c:
            c.execute("UPDATE tokens SET last_used=?, fail_count=0 WHERE refresh_token=?", (time.time(), t))

    def mark_failed(self, t: str) -> None:
        with self._conn() as c:
            c.execute("UPDATE tokens SET fail_count=fail_count+1 WHERE refresh_token=?", (t,))

    def mark_dead(self, t: str) -> None:
        with self._conn() as c:
            c.execute("UPDATE tokens SET dead=1 WHERE refresh_token=?", (t,))
            c.execute("DELETE FROM access_tokens WHERE refresh_token=?", (t,))

    def get_access_token(self, refresh_token: str) -> tuple[str, float] | None:
        with self._conn() as c:
            row = c.execute(
                "SELECT access_token, expires_at FROM access_tokens WHERE refresh_token=?",
                (refresh_token,),
            ).fetchone()
        if not row:
            return None
        return row["access_token"], row["expires_at"]

    def save_access_token(self, refresh_token: str, access: str, expires_at: float) -> None:
        with self._conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO access_tokens VALUES (?,?,?,?)",
                (refresh_token, access, expires_at, time.time()),
            )

    def already_done(self, t: str, ch: str) -> bool:
        with self._conn() as c:
            row = c.execute(
                "SELECT status FROM subs WHERE refresh_token=? AND channel_id=?", (t, ch)
            ).fetchone()
        return row is not None and row["status"] in ("subscribed", "already")

    def record_sub(self, t: str, ch: str, status: str) -> None:
        with self._conn() as c:
            c.execute("INSERT OR REPLACE INTO subs VALUES (?,?,?,?)", (t, ch, status, time.time()))

    def charge_quota(self, proj: str, units: int) -> int:
        today = time.strftime("%Y-%m-%d")
        with self._conn() as c:
            row = c.execute("SELECT day, used_units FROM project_quota WHERE project_id=?", (proj,)).fetchone()
            if not row or row["day"] != today:
                c.execute("INSERT OR REPLACE INTO project_quota VALUES (?,?,?)", (proj, today, units))
                return units
            new = row["used_units"] + units
            c.execute("UPDATE project_quota SET used_units=? WHERE project_id=?", (new, proj))
            return new

    def quota_remaining(self, proj: str) -> int:
        today = time.strftime("%Y-%m-%d")
        with self._conn() as c:
            row = c.execute("SELECT day, used_units FROM project_quota WHERE project_id=?", (proj,)).fetchone()
        if not row or row["day"] != today:
            return DEFAULT_DAILY_CAP
        return max(0, DEFAULT_DAILY_CAP - row["used_units"])

    def token_count(self) -> tuple[int, int]:
        with self._conn() as c:
            alive = c.execute("SELECT COUNT(*) FROM tokens WHERE dead=0").fetchone()[0]
            dead = c.execute("SELECT COUNT(*) FROM tokens WHERE dead=1").fetchone()[0]
        return alive, dead


# ---------------------------------------------------------------------------
# AUTH
# ---------------------------------------------------------------------------


class TokenError(Exception):
    """Refresh token dead. Re-mint required."""


class TokenCache:
    def __init__(self, store: Store):
        self.store = store
        self._mem: dict[str, tuple[str, float]] = {}

    def _fetch(self, refresh: str, client_id: str, client_secret: str) -> tuple[str, float]:
        r = requests.post(
            TOKEN_URL,
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh,
                "grant_type": "refresh_token",
            },
            timeout=20,
        )
        if r.status_code != 200:
            if "invalid_grant" in r.text.lower():
                raise TokenError("invalid_grant")
            r.raise_for_status()
        data = r.json()
        return data["access_token"], time.time() + int(data.get("expires_in", 3600))

    def get(self, refresh: str, client_id: str, client_secret: str, force: bool = False) -> str:
        if not force:
            hit = self._mem.get(refresh)
            if hit and hit[1] > time.time() + ACCESS_TOKEN_SAFETY:
                return hit[0]
            disk = self.store.get_access_token(refresh)
            if disk and disk[1] > time.time() + ACCESS_TOKEN_SAFETY:
                self._mem[refresh] = disk
                return disk[0]

        last_exc: Exception | None = None
        for attempt in range(AUTH_RETRIES):
            try:
                access, expires = self._fetch(refresh, client_id, client_secret)
                self._mem[refresh] = (access, expires)
                self.store.save_access_token(refresh, access, expires)
                return access
            except TokenError:
                raise
            except requests.RequestException as e:
                last_exc = e
                if attempt < AUTH_RETRIES - 1:
                    time.sleep(2 ** attempt)
        raise RuntimeError(f"auth fetch failed after {AUTH_RETRIES} attempts: {last_exc}")

    def pre_warm(self, token_rows: list[sqlite3.Row], proj_by_id: dict[str, GcpProject], ahead_seconds: int = PREWARM_WINDOW) -> int:
        now = time.time()
        refreshed = 0
        for row in token_rows:
            refresh = row["refresh_token"]
            proj = proj_by_id.get(row["project_id"])
            if not proj:
                continue
            hit = self._mem.get(refresh) or self.store.get_access_token(refresh)
            if hit and hit[1] > now + ahead_seconds:
                continue
            try:
                self.get(refresh, proj.client_id, proj.client_secret, force=True)
                refreshed += 1
            except TokenError:
                log.info(f"dead during pre-warm: {refresh[:12]}…")
                self.store.mark_dead(refresh)
            except Exception as e:
                log.warning(f"pre-warm failed {refresh[:12]}…: {e}")
        return refreshed


# ---------------------------------------------------------------------------
# PROXY
# ---------------------------------------------------------------------------


class ProxyPool:
    def __init__(self, raw: list[str]):
        norm = [p if "://" in p else f"http://{p}" for p in raw]
        self._cycle = cycle(norm) if norm else None
        self._size = len(norm)

    def next(self) -> dict | None:
        if not self._cycle:
            return None
        p = next(self._cycle)
        return {"http": p, "https": p}

    def __len__(self) -> int:
        return self._size


# ---------------------------------------------------------------------------
# OAUTH SUBSCRIBER
# ---------------------------------------------------------------------------


@dataclass
class SubResult:
    status: str
    detail: str = ""


def subscribe(access_token: str, channel_id: str, proxies: dict | None) -> SubResult:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    body = {"snippet": {"resourceId": {"kind": "youtube#channel", "channelId": channel_id}}}
    try:
        r = requests.post(SUB_API, params={"part": "snippet"}, json=body, headers=headers, proxies=proxies, timeout=30)
    except requests.RequestException as e:
        return SubResult("error", f"network: {e}")

    if r.status_code == 200:
        return SubResult("subscribed")

    try:
        err = r.json().get("error", {})
        reasons = {e.get("reason") for e in err.get("errors", [])}
        msg = err.get("message", "")
    except Exception:
        reasons, msg = set(), r.text[:200]

    if "quotaExceeded" in reasons:
        return SubResult("quota", msg)
    if "subscriptionForbidden" in reasons:
        return SubResult("forbidden", msg)
    if "rateLimitExceeded" in reasons or r.status_code == 429:
        return SubResult("rate", msg)
    if r.status_code == 401:
        return SubResult("auth", msg)
    if r.status_code == 409:
        return SubResult("already", msg)
    return SubResult("error", f"{r.status_code}: {msg}")


# ---------------------------------------------------------------------------
# COOKIE SUBSCRIBER (InnerTube)
# ---------------------------------------------------------------------------


class CookieSession:
    def __init__(self, cookie_blob: str, proxy: dict | None = None):
        self.cookies = self._parse(cookie_blob)
        self.sapisid = (
            self.cookies.get("__Secure-3PAPISID")
            or self.cookies.get("SAPISID")
            or self.cookies.get("__Secure-1PAPISID")
        )
        if not self.sapisid:
            raise ValueError("cookie blob missing SAPISID / __Secure-3PAPISID")

        self.proxy = proxy
        self.session = requests.Session()
        self.session.cookies.update(self.cookies)
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://www.youtube.com",
            "Referer": "https://www.youtube.com/",
        })

    @staticmethod
    def _parse(blob: str) -> dict[str, str]:
        out: dict[str, str] = {}
        for line in blob.replace("\n", "; ").split(";"):
            line = line.strip()
            if not line or "=" not in line:
                continue
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
        return out

    def _auth_header(self) -> str:
        ts = int(time.time())
        digest = hashlib.sha1(f"{ts} {self.sapisid} https://www.youtube.com".encode()).hexdigest()
        return f"SAPISIDHASH {ts}_{digest}"

    def subscribe(self, channel_id: str) -> SubResult:
        headers = {
            "Authorization": self._auth_header(),
            "X-Origin": "https://www.youtube.com",
            "Content-Type": "application/json",
        }
        body = {
            "context": {"client": {"clientName": "WEB", "clientVersion": CLIENT_VERSION, "hl": "en", "gl": "US"}},
            "channelIds": [channel_id],
        }
        try:
            r = self.session.post(
                INNERTUBE_SUB,
                params={"key": INNERTUBE_KEY, "prettyPrint": "false"},
                json=body,
                headers=headers,
                proxies=self.proxy,
                timeout=30,
            )
        except requests.RequestException as e:
            return SubResult("error", f"network: {e}")

        if r.status_code == 200:
            return SubResult("subscribed" if r.json().get("actions") else "already")
        if r.status_code in (401, 403):
            return SubResult("auth", f"{r.status_code}: {r.text[:120]}")
        if r.status_code == 429:
            return SubResult("rate", r.text[:120])
        return SubResult("error", f"{r.status_code}: {r.text[:200]}")


# ---------------------------------------------------------------------------
# RESOLVER
# ---------------------------------------------------------------------------


def resolve_channel(access: str, handle: str, proxies: dict | None = None) -> tuple[str | None, str]:
    h = handle if handle.startswith("@") else f"@{handle}"
    r = requests.get(
        CHANNELS_API,
        params={"part": "id", "forHandle": h},
        headers={"Authorization": f"Bearer {access}"},
        proxies=proxies,
        timeout=20,
    )
    if r.status_code == 200 and r.json().get("items"):
        return r.json()["items"][0]["id"], "forHandle"

    r = requests.get(
        SEARCH_API,
        params={"part": "snippet", "q": handle, "type": "channel", "maxResults": 1},
        headers={"Authorization": f"Bearer {access}"},
        proxies=proxies,
        timeout=20,
    )
    if r.status_code == 200 and r.json().get("items"):
        return r.json()["items"][0]["snippet"]["channelId"], "search"
    return None, "none"


# ---------------------------------------------------------------------------
# SIGNAL
# ---------------------------------------------------------------------------


def _sig(_s, _f):
    global STOP
    STOP = True
    log.info("shutdown requested")


signal.signal(signal.SIGINT, _sig)
signal.signal(signal.SIGTERM, _sig)


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------


def _ingest_tokens(cfg: Config, store: Store) -> None:
    for entry in cfg.tokens.split(","):
        entry = entry.strip()
        if not entry or ":" not in entry:
            continue
        token, proj = entry.rsplit(":", 1)
        store.add_token(token, proj)


def _first_access(cfg: Config, store: Store) -> tuple[str, GcpProject] | None:
    rows = store.all_alive_tokens()
    if not rows:
        return None
    proj_by_id = {p.project_id: p for p in cfg.projects}
    cache = TokenCache(store)
    for row in rows:
        proj = proj_by_id.get(row["project_id"])
        if not proj:
            continue
        try:
            return cache.get(row["refresh_token"], proj.client_id, proj.client_secret), proj
        except TokenError:
            store.mark_dead(row["refresh_token"])
        except Exception as e:
            log.warning(f"auth error on {row['refresh_token'][:12]}…: {e}")
    return None


# ---------------------------------------------------------------------------
# COMMANDS
# ---------------------------------------------------------------------------


def cmd_run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    cfg = load_config()
    store = Store(cfg.db_path)

    pool = ProxyPool(cfg.proxies)
    if len(cfg.proxies) == 0:
        log.warning("no proxies configured — Codespaces IP will get rate-limited fast")

    _ingest_tokens(cfg, store)

    alive, dead = store.token_count()
    log.info(f"tokens: {alive} alive, {dead} dead | targets: {len(cfg.target_channels)} | proxies: {len(cfg.proxies)}")
    if alive == 0:
        log.error("no alive tokens — populate TOKENS env")
        return

    proj_by_id = {p.project_id: p for p in cfg.projects}
    cache = TokenCache(store)

    warmed = cache.pre_warm(store.all_alive_tokens(), proj_by_id)
    if warmed:
        log.info(f"pre-warmed {warmed} access tokens")

    while not STOP:
        did_work = False
        cache.pre_warm(store.all_alive_tokens(), proj_by_id)

        for row in store.available_tokens(cfg.account_cooldown):
            if STOP:
                break
            refresh = row["refresh_token"]
            proj_id = row["project_id"]
            proj = proj_by_id.get(proj_id)
            if not proj:
                continue
            if store.quota_remaining(proj_id) < QUOTA_PER_SUB:
                continue

            target = next((ch for ch in cfg.target_channels if not store.already_done(refresh, ch)), None)
            if target is None:
                continue

            try:
                access = cache.get(refresh, proj.client_id, proj.client_secret)
            except TokenError:
                log.info(f"dead (invalid_grant): {refresh[:12]}…")
                store.mark_dead(refresh)
                continue
            except Exception as e:
                log.warning(f"auth error: {e}")
                store.mark_failed(refresh)
                continue

            store.charge_quota(proj_id, QUOTA_PER_SUB)
            result = subscribe(access, target, proxies=pool.next())

            if result.status == "subscribed":
                store.record_sub(refresh, target, "subscribed")
                store.mark_used(refresh)
                log.info(f"OK   {refresh[:12]}… → {target}")
                did_work = True
            elif result.status == "already":
                store.record_sub(refresh, target, "already")
                store.mark_used(refresh)
                log.info(f"DUP  {refresh[:12]}… → {target}")
                did_work = True
            elif result.status in ("forbidden", "auth"):
                log.info(f"{result.status.upper()} {refresh[:12]}…")
                store.mark_dead(refresh)
            elif result.status == "quota":
                log.info(f"QUOTA exhausted on {proj_id}")
            elif result.status == "rate":
                log.info(f"RATE {refresh[:12]}…, backing off")
                store.mark_failed(refresh)
                time.sleep(random.uniform(60, 120))
            else:
                log.warning(f"ERR  {refresh[:12]}… {result.detail[:120]}")
                store.mark_failed(refresh)

            time.sleep(cfg.global_delay + random.uniform(0, cfg.global_delay * 0.5))

        if not did_work:
            log.info("idle — sleeping 5 min")
            for _ in range(30):
                if STOP:
                    break
                time.sleep(10)
    log.info("stopped")


def cmd_cookies():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    cfg = load_config(require_projects=False)

    raw = os.environ.get("COOKIES", "").strip()
    if not raw:
        raise SystemExit("COOKIES env is empty")

    blobs = [b.strip() for b in raw.split("\n---\n") if b.strip()]
    pool = ProxyPool(cfg.proxies)
    if len(cfg.proxies) == 0:
        log.warning("no proxies — datacenter IP will get rate-limited fast")

    log.info(f"accounts: {len(blobs)} | targets: {len(cfg.target_channels)} | proxies: {len(cfg.proxies)}")

    sessions: list[CookieSession] = []
    for i, blob in enumerate(blobs):
        try:
            sessions.append(CookieSession(blob))
        except ValueError as e:
            log.warning(f"account {i}: {e}")
    if not sessions:
        raise SystemExit("no usable cookie sessions")

    store = Store(cfg.db_path)

    def key(s: CookieSession) -> str:
        return "ck:" + hashlib.sha1(s.sapisid.encode()).hexdigest()[:24]

    for s in sessions:
        store.add_token(key(s), "cookies")

    while not STOP:
        did_work = False
        for s in sessions:
            if STOP:
                break
            k = key(s)
            target = next((ch for ch in cfg.target_channels if not store.already_done(k, ch)), None)
            if target is None:
                continue

            result = s.subscribe(target)
            if result.status == "subscribed":
                store.record_sub(k, target, "subscribed")
                store.mark_used(k)
                log.info(f"OK   {k} → {target}")
                did_work = True
            elif result.status == "already":
                store.record_sub(k, target, "already")
                store.mark_used(k)
                log.info(f"DUP  {k} → {target}")
                did_work = True
            elif result.status == "auth":
                log.info(f"AUTH {k} — cookies expired")
                store.mark_dead(k)
            elif result.status == "rate":
                log.info(f"RATE {k}, backing off")
                time.sleep(random.uniform(60, 120))
            else:
                log.warning(f"ERR  {k} {result.detail[:120]}")

            time.sleep(cfg.global_delay + random.uniform(0, cfg.global_delay * 0.5))

        if not did_work:
            log.info("idle — sleeping 5 min")
            for _ in range(30):
                if STOP:
                    break
                time.sleep(10)


def cmd_status():
    cfg = load_config()
    store = Store(cfg.db_path)
    _ingest_tokens(cfg, store)

    alive, dead = store.token_count()
    print(f"alive tokens : {alive}")
    print(f"dead tokens  : {dead}")
    print(f"targets      : {len(cfg.target_channels)}")
    print(f"proxies      : {len(cfg.proxies)}")

    now = time.time()
    with store._conn() as c:
        tokens = c.execute("SELECT refresh_token FROM tokens WHERE dead=0").fetchall()
        cached = stale = 0
        for t in tokens:
            row = c.execute("SELECT expires_at FROM access_tokens WHERE refresh_token=?", (t["refresh_token"],)).fetchone()
            if row and row["expires_at"] > now + ACCESS_TOKEN_SAFETY:
                cached += 1
            else:
                stale += 1
        qrows = c.execute("SELECT project_id, used_units, day FROM project_quota").fetchall()

    print(f"access cached: {cached} fresh, {stale} need refresh")
    for r in qrows:
        remaining = max(0, DEFAULT_DAILY_CAP - r["used_units"])
        print(f"  quota {r['project_id']} ({r['day']}): used {r['used_units']}, remaining {remaining}")


def cmd_cookie_status():
    cfg = load_config(require_projects=False)
    store = Store(cfg.db_path)
    with store._conn() as c:
        rows = c.execute("SELECT refresh_token, dead, last_used FROM tokens WHERE refresh_token LIKE 'ck:%'").fetchall()
    print(f"cookie accounts: {len(rows)}")
    for r in rows:
        state = "dead" if r["dead"] else "alive"
        when = time.strftime("%H:%M:%S", time.localtime(r["last_used"])) if r["last_used"] else "never"
        print(f"  {r['refresh_token']}  {state}  last used {when}")


def cmd_refresh():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    cfg = load_config()
    store = Store(cfg.db_path)
    _ingest_tokens(cfg, store)
    proj_by_id = {p.project_id: p for p in cfg.projects}
    cache = TokenCache(store)
    rows = store.all_alive_tokens()
    print(f"refreshing {len(rows)} tokens...")
    n = cache.pre_warm(rows, proj_by_id, ahead_seconds=999_999)
    print(f"refreshed {n} / {len(rows)}")


def cmd_resolve():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    if len(sys.argv) < 3:
        print("usage: python3 sub_bot.py resolve @Handle")
        sys.exit(1)
    handle = sys.argv[2]
    cfg = load_config()
    store = Store(cfg.db_path)
    _ingest_tokens(cfg, store)

    picked = _first_access(cfg, store)
    if not picked:
        raise SystemExit("no usable tokens — mint one first, or check TOKENS env")
    access, _ = picked

    ch_id, method = resolve_channel(access, handle)
    if ch_id:
        print(ch_id)
        print(f"method: {method} (cost: {'1' if method == 'forHandle' else '100'} quota units)", file=sys.stderr)
    else:
        print(f"could not resolve {handle!r}", file=sys.stderr)
        sys.exit(2)


def cmd_mint():
    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        raise SystemExit("pip install google-auth-oauthlib")

    if len(sys.argv) < 4:
        print("usage: python3 sub_bot.py mint CLIENT_ID CLIENT_SECRET")
        sys.exit(1)
    client_id, client_secret = sys.argv[2], sys.argv[3]
    cfg = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": TOKEN_URL,
            "redirect_uris": ["http://localhost"],
        }
    }
    flow = InstalledAppFlow.from_client_config(cfg, ["https://www.googleapis.com/auth/youtube.force-ssl"])
    creds = flow.run_local_server(port=0, prompt="consent", access_type="offline")
    print("\n--- refresh token ---")
    print(creds.refresh_token)
    print("---------------------\n")
    print(f"Add to TOKENS env: {creds.refresh_token}:<project_id>")


# ---------------------------------------------------------------------------
# ENTRY
# ---------------------------------------------------------------------------

USAGE = """sub_bot.py — YouTube subscriber pool

  python3 sub_bot.py run              OAuth loop (GCP / Data API v3)
  python3 sub_bot.py cookies          cookie loop (InnerTube, no GCP)
  python3 sub_bot.py mint ID SECRET   mint a refresh token (local)
  python3 sub_bot.py resolve @Handle  handle → UC channel ID
  python3 sub_bot.py refresh          force-refresh every access token
  python3 sub_bot.py status           OAuth pool + cache + quota stats
  python3 sub_bot.py cookie-status    cookie-mode account stats
"""


def main():
    if len(sys.argv) < 2:
        print(USAGE)
        sys.exit(0)
    cmd = sys.argv[1]
    if cmd == "run":
        cmd_run()
    elif cmd == "cookies":
        cmd_cookies()
    elif cmd == "mint":
        cmd_mint()
    elif cmd == "resolve":
        cmd_resolve()
    elif cmd == "refresh":
        cmd_refresh()
    elif cmd == "status":
        cmd_status()
    elif cmd == "cookie-status":
        cmd_cookie_status()
    else:
        print(USAGE)
        sys.exit(1)


if __name__ == "__main__":
    main()
