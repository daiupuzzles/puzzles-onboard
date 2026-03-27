"""Doppler secret access for Telegram wrapper."""

import json
import subprocess
from typing import Optional

_cache: Optional[dict] = None


def get_secrets(project: str = "flowsly", config: str = "prd") -> dict:
    """Fetch all secrets from a Doppler project as a dict.

    Results are cached after first call to avoid repeated subprocess invocations.

    Args:
        project: Doppler project (default: "flowsly")
        config: Doppler config (default: "prd")

    Returns:
        Dict of secret_key -> secret_value

    Raises:
        RuntimeError: If Doppler command fails
    """
    global _cache
    if _cache is not None:
        return _cache

    result = subprocess.run(
        ["doppler", "secrets", "download", "--project", project, "--config", config,
         "--format", "json", "--no-file"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Doppler failed for {project}/{config}: {result.stderr.strip()}")

    _cache = json.loads(result.stdout)
    return _cache


def get_telegram_config() -> dict:
    """Get Telegram bot configuration from Doppler.

    Returns:
        dict with: bot_token, errors_chat_id
    """
    secrets = get_secrets()
    return {
        "bot_token": secrets.get("TELEGRAM_BOT_TOKEN"),
        "errors_chat_id": secrets.get("TELEGRAM_ERRORS_CHAT_ID"),
    }


def get_outreach_config() -> dict:
    """Get Telegram outreach bot configuration from Doppler.

    Returns:
        dict with: bot_token, chat_id
    """
    secrets = get_secrets()
    return {
        "bot_token": secrets.get("TELEGRAM_OUTREACH_BOT_TOKEN"),
        "chat_id": secrets.get("TELEGRAM_OUTREACH_CHAT_ID"),
    }
