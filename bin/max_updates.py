#!/usr/bin/env python3
"""
Read recent MAX bot updates.

Usage:
    MAX_BOT_TOKEN="..." ./bin/max_updates.py
"""

import json
import os
import requests


MAX_API_URL = os.getenv("MAX_API_URL", "https://platform-api.max.ru")
MAX_BOT_TOKEN = os.getenv("MAX_BOT_TOKEN", "").strip()


def main() -> None:
    if not MAX_BOT_TOKEN:
        raise SystemExit("MAX_BOT_TOKEN is empty")

    response = requests.get(
        f"{MAX_API_URL}/updates",
        headers={"Authorization": MAX_BOT_TOKEN},
        params={"limit": 10},
        timeout=30,
    )

    response.raise_for_status()

    print(json.dumps(response.json(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()