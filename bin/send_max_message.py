#!/usr/bin/env python3
"""
Send plain text message to MAX messenger.

Env:
- MAX_BOT_TOKEN
- MAX_CHAT_ID or MAX_USER_ID

Usage:
    echo "Hello from Metrigo" | ./bin/send_max_message.py
    ./bin/send_max_message.py "Hello from Metrigo"
"""

import os
import sys
import requests


MAX_API_URL = os.getenv("MAX_API_URL", "https://platform-api.max.ru/messages")
MAX_BOT_TOKEN = os.getenv("MAX_BOT_TOKEN", "").strip()
MAX_CHAT_ID = os.getenv("MAX_CHAT_ID", "").strip()
MAX_USER_ID = os.getenv("MAX_USER_ID", "").strip()


def read_message() -> str:
    """
    Read message from argv or stdin.
    """
    if len(sys.argv) > 1:
        return " ".join(sys.argv[1:]).strip()

    if not sys.stdin.isatty():
        return sys.stdin.read().strip()

    return ""


def main() -> None:
    """
    Send message to MAX.
    """
    if not MAX_BOT_TOKEN:
        raise SystemExit("MAX_BOT_TOKEN is empty")

    params = {}

    if MAX_CHAT_ID:
        params["chat_id"] = MAX_CHAT_ID
    elif MAX_USER_ID:
        params["user_id"] = MAX_USER_ID
    else:
        raise SystemExit("MAX_CHAT_ID or MAX_USER_ID is required")

    text = read_message()

    if not text:
        raise SystemExit("Message text is empty")

    response = requests.post(
        MAX_API_URL,
        headers={
            "Authorization": MAX_BOT_TOKEN,
            "Content-Type": "application/json",
        },
        params=params,
        json={
            "text": text[:4000],
            "notify": True,
        },
        timeout=30,
    )

    response.raise_for_status()

    print("sent")


if __name__ == "__main__":
    main()