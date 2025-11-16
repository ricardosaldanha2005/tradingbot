# pip install python-dotenv requests

import os
import time
import hmac
import hashlib
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv

load_dotenv()

BINANCE_MODE = os.getenv("BINANCE_MODE", "").upper()
API_KEY = os.getenv("BINANCE_API_KEY_TEST", "")
API_SECRET = os.getenv("BINANCE_API_SECRET_TEST", "").encode()

BASE_URL = "https://testnet.binance.vision"


def signed_request(method: str, path: str, params: dict | None = None):
    if params is None:
        params = {}

    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 5000

    query_string = urlencode(params, doseq=True)
    signature = hmac.new(API_SECRET, query_string.encode(), hashlib.sha256).hexdigest()
    query_string += f"&signature={signature}"

    url = f"{BASE_URL}{path}?{query_string}"

    headers = {"X-MBX-APIKEY": API_KEY}
    resp = requests.request(method, url, headers=headers, timeout=10)

    if resp.status_code != 200:
        raise RuntimeError(f"Binance error {resp.status_code}: {resp.text}")

    return resp.json()


def public_request(path: str, params: dict | None = None):
    url = f"{BASE_URL}{path}"
    resp = requests.get(url, params=params, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"Binance public error {resp.status_code}: {resp.text}")
    return resp.json()


def main():
    if BINANCE_MODE != "TESTNET":
        raise SystemExit(f"BINANCE_MODE must be TESTNET, got: {BINANCE_MODE!r}")

    if not API_KEY or not API_SECRET:
        raise SystemExit("Missing BINANCE_API_KEY_TEST or BINANCE_API_SECRET_TEST in .env")

    print("-> Checking server time...")
    server_time = public_request("/api/v3/time")
    print("   Server time:", server_time)

    print("-> Fetching account info from TESTNET...")
    account = signed_request("GET", "/api/v3/account")

    balances = account.get("balances", [])
    non_zero = [b for b in balances if float(b["free"]) > 0 or float(b["locked"]) > 0]

    print("Non-zero balances on TESTNET:")
    for b in non_zero:
        print(f"  {b['asset']}: free={b['free']}, locked={b['locked']}")


if __name__ == "__main__":
    main()
