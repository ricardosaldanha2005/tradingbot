# pip install supabase==2.6.0 gotrue==2.4.2 storage3==0.7.6 httpx==0.27.2 python-dotenv

import os
from typing import Any, Optional

from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE = os.getenv("SUPABASE_SERVICE_ROLE", "")
SIGNALS_TABLE = os.getenv("SIGNALS_TABLE", "signals")


def get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
        raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE in env")
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE)


def fetch_next_pending_signal(sb: Client) -> Optional[dict[str, Any]]:
    query = (
        sb.table(SIGNALS_TABLE)
        .select("*")
        .eq("sent_to_exchange", False)
        .order("created_at", desc=True)
        .limit(1)
    )

    res = query.execute()
    data = res.data or []
    if not data:
        return None
    return data[0]


def main() -> None:
    print("-> Connecting to Supabase...")
    sb = get_supabase_client()

    print(f"-> Fetching 1 pending signal from table '{SIGNALS_TABLE}'...")
    signal = fetch_next_pending_signal(sb)

    if signal is None:
        print("No pending signals found (sent_to_exchange = false).")
        return

    print("Found signal:")
    for k, v in signal.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
