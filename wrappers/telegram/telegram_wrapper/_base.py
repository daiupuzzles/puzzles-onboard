"""
Telegram Bot API Base - Token management and API call infrastructure.
Uses bot token from Doppler flowsly/prd.
"""

import logging
from typing import Optional

import httpx

logger = logging.getLogger("telegram_wrapper")

_bot_token: Optional[str] = None


def use_bot(bot_token: str = None) -> str:
    """Set the bot token context.

    If bot_token is None, loads TELEGRAM_BOT_TOKEN from Doppler flowsly/prd.

    Args:
        bot_token: Telegram Bot API token. If None, auto-loads from Doppler.

    Returns:
        The bot token string.
    """
    global _bot_token

    if bot_token:
        _bot_token = bot_token
    else:
        from ._secrets import get_telegram_config
        config = get_telegram_config()
        _bot_token = config["bot_token"]
        if not _bot_token:
            raise RuntimeError(
                "TELEGRAM_BOT_TOKEN not found in Doppler flowsly/prd. "
                "Set it with: doppler secrets set TELEGRAM_BOT_TOKEN=... --project flowsly --config prd"
            )

    return _bot_token


def get_bot_token() -> str:
    """Return current bot token or auto-load from Doppler."""
    global _bot_token
    if not _bot_token:
        use_bot()
    return _bot_token


def _api_call(method: str, bot_token: str = None, **params) -> dict:
    """Make a Telegram Bot API call.

    Args:
        method: API method name (e.g., "sendMessage")
        bot_token: Optional bot token override. If None, uses default bot.
        **params: Method parameters

    Returns:
        Response JSON result dict

    Raises:
        RuntimeError: If API call fails
    """
    token = bot_token or get_bot_token()
    url = f"https://api.telegram.org/bot{token}/{method}"

    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.post(url, json=params)
            response.raise_for_status()
            data = response.json()

            if not data.get("ok"):
                raise RuntimeError(
                    f"Telegram API error: {data.get('description', 'Unknown error')} "
                    f"(error_code: {data.get('error_code')})"
                )

            return data.get("result", {})
    except httpx.HTTPStatusError as e:
        raise RuntimeError(
            f"Telegram API HTTP {e.response.status_code}: {e.response.text}"
        ) from e
