"""
Telegram message operations.
"""

import logging
from typing import Optional
from ._base import _api_call

logger = logging.getLogger("telegram_wrapper.messages")

MAX_ERROR_LENGTH = 700  # Fits on one screen without scrolling


def send_message(chat_id: str, text: str, parse_mode: str = "",
                 disable_notification: bool = False) -> dict:
    """Send a text message to a Telegram chat.

    Args:
        chat_id: Target chat ID
        text: Message text (max 4096 chars for Telegram)
        parse_mode: "HTML", "MarkdownV2", or "" for plain text
        disable_notification: Send silently

    Returns:
        Telegram Message object
    """
    params = {
        "chat_id": chat_id,
        "text": text,
        "disable_notification": disable_notification,
    }
    if parse_mode:
        params["parse_mode"] = parse_mode

    return _api_call("sendMessage", **params)


def create_forum_topic(chat_id: str, name: str, *,
                       icon_color: int = None,
                       bot_token: str = None) -> dict:
    """Create a forum topic in a Telegram supergroup.

    The supergroup must have forum topics enabled, and the bot must be
    an admin with "Manage Topics" permission.

    Args:
        chat_id: Supergroup chat ID
        name: Topic name (1-128 characters)
        icon_color: Optional icon color (0x6FB9F0, 0xFFD67E, 0xCB86DB,
                    0x8EEE98, 0xFF93B2, 0xFB6F5F)
        bot_token: Optional bot token override (e.g., outreach bot)

    Returns:
        Forum topic object with message_thread_id, name, icon_color
    """
    params = {"chat_id": chat_id, "name": name}
    if icon_color is not None:
        params["icon_color"] = icon_color

    result = _api_call("createForumTopic", bot_token=bot_token, **params)
    logger.info("Created forum topic '%s' (thread_id=%s) in %s",
                name, result.get("message_thread_id"), chat_id)
    return result


def send_to_topic(chat_id: str, message_thread_id: int, text: str, *,
                  parse_mode: str = "",
                  disable_notification: bool = False,
                  bot_token: str = None) -> dict:
    """Send a message to a specific forum topic in a Telegram supergroup.

    Args:
        chat_id: Supergroup chat ID
        message_thread_id: Forum topic thread ID (from create_forum_topic)
        text: Message text (max 4096 chars)
        parse_mode: "HTML", "MarkdownV2", or "" for plain text
        disable_notification: Send silently
        bot_token: Optional bot token override (e.g., outreach bot)

    Returns:
        Telegram Message object (includes message_id for telegram_message_map)
    """
    params = {
        "chat_id": chat_id,
        "message_thread_id": message_thread_id,
        "text": text,
        "disable_notification": disable_notification,
    }
    if parse_mode:
        params["parse_mode"] = parse_mode

    result = _api_call("sendMessage", bot_token=bot_token, **params)
    logger.info("Sent message to topic %s in %s (message_id=%s)",
                message_thread_id, chat_id, result.get("message_id"))
    return result


def send_error(source: str, workflow_name: str, short_message: str,
               details: str = "", chat_id: str = None) -> dict:
    """Send a formatted error notification to the #errors channel.

    Format:
        🔴 [{source}] {workflow_name}
        {short_message}

        {details_block}

    Truncated at 700 chars (one screen, no scrolling) if needed.

    Args:
        source: Error source (e.g., "n8n-flowsly", "windmill-puzzles", "modal")
        workflow_name: Name of the workflow/script that failed
        short_message: Brief error description
        details: Optional additional context (node name, execution ID, etc.)
        chat_id: Override chat ID. If None, uses TELEGRAM_ERRORS_CHAT_ID from Doppler.

    Returns:
        Telegram Message object
    """
    if not chat_id:
        from ._secrets import get_telegram_config
        config = get_telegram_config()
        chat_id = config["errors_chat_id"]
        if not chat_id:
            raise RuntimeError("TELEGRAM_ERRORS_CHAT_ID not found in Doppler flowsly/prd")

    header = f"\U0001f534 [{source}] {workflow_name}"
    body = short_message

    if details:
        message = f"{header}\n{body}\n\n{details}"
    else:
        message = f"{header}\n{body}"

    if len(message) > MAX_ERROR_LENGTH:
        message = message[:MAX_ERROR_LENGTH - 3] + "..."

    return send_message(chat_id=chat_id, text=message)
