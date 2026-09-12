#!/usr/bin/env bash
# setup.sh — cookie-mode subscriber + OAuth token mint, one file
set -e

# ─────────── EDIT THESE TWO ───────────
COOKIES="SID=g.a000CgkzFuulOaKtviolnSavXPO-J6-7L27rHzxAZIVqJi8qSRXAQvUUD7F_toyWotr-2x6KOAACgYKARUSARESFQHGX2MikWFiTeiGfuW9gHoXIx_lLRoVAUF8yKpa83eTzTX0o3BYIQR3_84O0076; HSID=Aw17odVI8MkJ3csLw; SSID=AABCPKs-fkNM6mc2V; APISID=WAakkZCPYN3GxH-w/AXD5MXFTf739J1eav; SAPISID=MvXj2A6ADWTaceqk/ABuZvc0kr0Y0-hspH; __Secure-3PAPISID=MvXj2A6ADWTaceqk/ABuZvc0kr0Y0-hspH"
TARGET_CHANNEL="UC24uzB7PL4PAgND79wrYDsA"
# ──────────────────────────────────────

DIR="$HOME/bot"
mkdir -p "$DIR"
cd "$DIR"

echo "[*] writing sub_bot.py ..."
cat > sub_bot.py << 'PYEOF'
#!/usr/bin/env python3
# sub_bot.py
#   python3 sub_bot.py            cookie-mode subscribe (default)
#   python3 sub_bot.py mint ID SECRET   OAuth refresh-token mint
import os, sys, time, hashlib
import requests

INNERTUBE_KEY = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"
INNERTUBE_SUB = "https://www.youtube.com/youtubei/v1/subscription/subscribe"
CLIENT_VERSION = "2.20240901.00.00"
TOKEN_URL = "https://oauth2.googleapis.com/token"


class Session:
    def __init__(self, blob):
        self.cookies = {}
        for part in blob.replace("\n", "; ").split(";"):
            part = part.strip()
            if "=" in part:
                k, v = part.split("=", 1)
                self.cookies[k.strip()] = v.strip()
        self.sapisid = (self.cookies.get("__Secure-3PAPISID")
                        or self.cookies.get("SAPISID")
                        or self.cookies.get("__Secure-1PAPISID"))
        if not self.sapisid:
            raise ValueError("missing SAPISID / __Secure-3PAPISID in cookie string")
        self.s = requests.Session()
        self.s.cookies.update(self.cookies)
        self.s.headers.update({
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

    def _auth(self):
        ts = int(time.time())
        d = hashlib.sha1(f"{ts} {self.sapisid} https://www.youtube.com".encode()).hexdigest()
        return f"SAPISIDHASH {ts}_{d}"

    def subscribe(self, ch):
        headers = {
            "Authorization": self._auth(),
            "X-Origin": "https://www.youtube.com",
            "Content-Type": "application/json",
        }
        body = {
            "context": {"client": {
                "clientName": "WEB",
                "clientVersion": CLIENT_VERSION,
                "hl": "en",
                "gl": "US",
            }},
            "channelIds": [ch],
        }
        try:
            r = self.s.post(
                INNERTUBE_SUB,
                params={"key": INNERTUBE_KEY, "prettyPrint": "false"},
                json=body,
                headers=headers,
                timeout=30,
            )
            return r.status_code, r.text[:400]
        except requests.RequestException as e:
            return 0, f"network: {e}"


def cmd_subscribe():
    blob = os.environ.get("COOKIES", "").strip()
    ch = os.environ.get("TARGET_CHANNEL", "").strip()
    if not blob:
        print("missing COOKIES env")
        sys.exit(1)
    if not ch:
        print("missing TARGET_CHANNEL env")
        sys.exit(1)
    try:
        s = Session(blob)
    except ValueError as e:
        print(f"cookie parse error: {e}")
        sys.exit(2)
    print(f"subscribing to {ch} ...")
    code, body = s.subscribe(ch)
    print(f"status: {code}")
    print(f"body: {body[:400]}")
    if code == 200:
        print("=> OK, subscribed"); sys.exit(0)
    elif code in (401, 403):
        print("=> auth failed — cookies expired or account flagged"); sys.exit(3)
    else:
        print("=> unexpected response, see body above"); sys.exit(4)


def cmd_mint():
    """Mint a refresh token. In Codespaces the browser can't open locally —
    the script prints a URL, Codespaces forwards port 8080, you open it in
    phone Chrome, sign in, and the redirect lands back here."""
    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        raise SystemExit("pip install google-auth-oauthlib")

    if len(sys.argv) < 4:
        print("usage: python3 sub_bot.py mint CLIENT_ID CLIENT_SECRET")
        sys.exit(1)
    cid, csec = sys.argv[2], sys.argv[3]
    cfg = {"installed": {
        "client_id": cid,
        "client_secret": csec,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": TOKEN_URL,
        "redirect_uris": ["http://localhost"],
    }}
    flow = InstalledAppFlow.from_client_config(
        cfg, ["https://www.googleapis.com/auth/youtube.force-ssl"])
    creds = flow.run_local_server(
        host="0.0.0.0", port=8080, open_browser=False,
        prompt="consent", access_type="offline")
    print("\n--- refresh token ---")
    print(creds.refresh_token)
    print("---------------------\n")
    print(f"Add to TOKENS env: {creds.refresh_token}:<project_id>")


def main():
    if len(sys.argv) >= 2 and sys.argv[1] == "mint":
        cmd_mint()
    else:
        cmd_subscribe()


if __name__ == "__main__":
    main()
PYEOF
chmod +x sub_bot.py

echo "[*] writing .env ..."
cat > .env << EOF
export COOKIES='${COOKIES}'
export TARGET_CHANNEL='${TARGET_CHANNEL}'
EOF
chmod 600 .env

echo "[*] installing deps ..."
pip install --quiet requests google-auth-oauthlib

echo "[*] running subscribe once ..."
source .env
python3 sub_bot.py

echo ""
echo "=============================================="
echo " subscribe : cd ~/bot && source .env && python3 sub_bot.py"
echo " mint      : cd ~/bot && python3 sub_bot.py mint CLIENT_ID CLIENT_SECRET"
echo "=============================================="
