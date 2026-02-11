"""Shared utility functions."""

import logging

logger = logging.getLogger("tg-cw-bridge")


def safe_json(resp) -> dict:
    try:
        return resp.json()
    except Exception:
        return {}
