"""
Gmail Operations — Send, read, thread, label, history, and parsing functions.
All functions default to the current account context via _base.get_current_account().
"""

import base64
import logging
import re
import time
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from googleapiclient.errors import HttpError

from ._base import (
    get_gmail_service,
    get_current_account,
    get_account_email,
    api_call_with_retry,
)

logger = logging.getLogger("google_wrapper.gmail")

OUR_EMAILS = {
    "daniel@flowsly.io",
    "daniel@flowsly.ai",
    "daniel@puzzles.consulting",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _encode_message(mime_msg) -> str:
    """Encode MIME message to URL-safe base64 string."""
    raw = base64.urlsafe_b64encode(mime_msg.as_bytes()).decode("ascii")
    return raw


def _build_mime(to, subject, body, html=None, cc=None, bcc=None,
                in_reply_to=None, references=None, from_email=None,
                attachments=None):
    """Build a MIME message.

    Args:
        attachments: Optional list of (filename, content_bytes, mime_type) tuples.
            Example: [("report.pdf", pdf_bytes, "application/pdf")]
    """
    # Build the text/html content part
    if html:
        text_part = MIMEMultipart("alternative")
        text_part.attach(MIMEText(body, "plain"))
        text_part.attach(MIMEText(html, "html"))
    else:
        text_part = MIMEText(body, "plain")

    # If attachments, wrap in mixed container
    if attachments:
        msg = MIMEMultipart("mixed")
        msg.attach(text_part)
        for filename, content_bytes, mime_type in attachments:
            if not mime_type or "/" not in mime_type:
                raise ValueError(f"Invalid mime_type for attachment '{filename}': {mime_type!r}")
            main_type, sub_type = mime_type.split("/", 1)
            part = MIMEBase(main_type, sub_type)
            part.set_payload(content_bytes)
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", "attachment", filename=filename)
            msg.attach(part)
    else:
        msg = text_part

    msg["To"] = to if isinstance(to, str) else ", ".join(to)
    msg["Subject"] = subject
    if from_email:
        msg["From"] = from_email
    if cc:
        msg["Cc"] = cc if isinstance(cc, str) else ", ".join(cc)
    if bcc:
        msg["Bcc"] = bcc if isinstance(bcc, str) else ", ".join(bcc)
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
    if references:
        msg["References"] = references

    return msg


# ---------------------------------------------------------------------------
# Send / Draft
# ---------------------------------------------------------------------------

def send_email(to, subject, body, html=None, cc=None, bcc=None, account=None) -> str:
    """Send an email. Returns message ID."""
    service = get_gmail_service(account)
    from_email = get_account_email(account)

    mime = _build_mime(to, subject, body, html=html, cc=cc, bcc=bcc, from_email=from_email)
    raw = _encode_message(mime)

    result = api_call_with_retry(
        service.users().messages().send(userId="me", body={"raw": raw}).execute
    )
    msg_id = result["id"]
    logger.info("Sent email to=%s subject='%s' id=%s", to, subject, msg_id)
    return msg_id


def create_draft(to, subject, body, html=None, cc=None, bcc=None,
                 attachments=None, account=None) -> str:
    """Create an email draft. Returns draft ID.

    Args:
        attachments: Optional list of (filename, content_bytes, mime_type) tuples.
    """
    service = get_gmail_service(account)
    from_email = get_account_email(account)

    mime = _build_mime(to, subject, body, html=html, cc=cc, bcc=bcc,
                       attachments=attachments, from_email=from_email)
    raw = _encode_message(mime)

    result = api_call_with_retry(
        service.users().drafts().create(
            userId="me", body={"message": {"raw": raw}}
        ).execute
    )
    draft_id = result["id"]
    logger.info("Created draft to=%s subject='%s' draft_id=%s", to, subject, draft_id)
    return draft_id


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------

def get_messages(query=None, max_results=100, label_ids=None, account=None) -> list:
    """List messages matching query. Returns list of message dicts (id, threadId)."""
    service = get_gmail_service(account)
    messages = []
    page_token = None

    while len(messages) < max_results:
        kwargs = {"userId": "me", "maxResults": min(max_results - len(messages), 500)}
        if query:
            kwargs["q"] = query
        if label_ids:
            kwargs["labelIds"] = label_ids
        if page_token:
            kwargs["pageToken"] = page_token

        resp = api_call_with_retry(
            service.users().messages().list(**kwargs).execute
        )
        batch = resp.get("messages", [])
        messages.extend(batch)

        page_token = resp.get("nextPageToken")
        if not page_token or not batch:
            break

    return messages[:max_results]


def get_message(message_id, format="full", account=None) -> dict:
    """Get a single message by ID."""
    service = get_gmail_service(account)
    return api_call_with_retry(
        service.users().messages().get(userId="me", id=message_id, format=format).execute
    )


# ---------------------------------------------------------------------------
# Threads
# ---------------------------------------------------------------------------

def list_threads(query=None, max_results=100, label_ids=None,
                 page_token=None, account=None) -> dict:
    """List threads with pagination. Returns {threads: [...], nextPageToken: str|None}."""
    service = get_gmail_service(account)

    kwargs = {"userId": "me", "maxResults": min(max_results, 500)}
    if query:
        kwargs["q"] = query
    if label_ids:
        kwargs["labelIds"] = label_ids
    if page_token:
        kwargs["pageToken"] = page_token

    resp = api_call_with_retry(
        service.users().threads().list(**kwargs).execute
    )
    return {
        "threads": resp.get("threads", []),
        "nextPageToken": resp.get("nextPageToken"),
    }


def get_thread(thread_id, format="full", account=None) -> dict:
    """Fetch complete thread with all messages."""
    service = get_gmail_service(account)
    return api_call_with_retry(
        service.users().threads().get(userId="me", id=thread_id, format=format).execute
    )


def reply_to_thread(thread_id, body, html=None, to=None, cc=None, account=None) -> str:
    """Reply to a thread, preserving threading headers. Returns message ID.

    Auto-sets In-Reply-To, References, and Subject from the last message.
    If `to` is not provided, replies to the sender of the last message.
    """
    service = get_gmail_service(account)
    from_email = get_account_email(account)

    # Fetch thread to get last message headers
    thread = get_thread(thread_id, format="metadata", account=account)
    messages = thread.get("messages", [])
    if not messages:
        raise ValueError(f"Thread {thread_id} has no messages")

    last_msg = messages[-1]
    headers = parse_message_headers(last_msg)

    # Build threading headers
    last_message_id = headers.get("Message-ID", "")
    existing_refs = headers.get("References", "")
    references = f"{existing_refs} {last_message_id}".strip() if existing_refs else last_message_id

    # Subject — add Re: if not present
    subject = headers.get("Subject", "")
    if subject and not subject.lower().startswith("re:"):
        subject = f"Re: {subject}"

    # Default recipient: sender of last message (unless it's us)
    if not to:
        last_from = headers.get("From", "")
        last_to = headers.get("To", "")
        # If last message was from us, reply to the To field instead
        from_addr = _extract_email_address(last_from)
        if from_addr and from_addr.lower() in {e.lower() for e in OUR_EMAILS}:
            to = last_to
        else:
            to = last_from

    mime = _build_mime(
        to=to, subject=subject, body=body, html=html, cc=cc,
        in_reply_to=last_message_id, references=references,
        from_email=from_email,
    )
    raw = _encode_message(mime)

    result = api_call_with_retry(
        service.users().messages().send(
            userId="me", body={"raw": raw, "threadId": thread_id}
        ).execute
    )
    msg_id = result["id"]
    logger.info("Replied to thread=%s msg_id=%s", thread_id, msg_id)
    return msg_id


def create_draft_reply(thread_id, body, html=None, to=None, account=None) -> str:
    """Create a draft reply in an existing thread. Returns draft ID."""
    service = get_gmail_service(account)
    from_email = get_account_email(account)

    thread = get_thread(thread_id, format="metadata", account=account)
    messages = thread.get("messages", [])
    if not messages:
        raise ValueError(f"Thread {thread_id} has no messages")

    last_msg = messages[-1]
    headers = parse_message_headers(last_msg)

    last_message_id = headers.get("Message-ID", "")
    existing_refs = headers.get("References", "")
    references = f"{existing_refs} {last_message_id}".strip() if existing_refs else last_message_id

    subject = headers.get("Subject", "")
    if subject and not subject.lower().startswith("re:"):
        subject = f"Re: {subject}"

    if not to:
        last_from = headers.get("From", "")
        last_to = headers.get("To", "")
        from_addr = _extract_email_address(last_from)
        if from_addr and from_addr.lower() in {e.lower() for e in OUR_EMAILS}:
            to = last_to
        else:
            to = last_from

    mime = _build_mime(
        to=to, subject=subject, body=body, html=html,
        in_reply_to=last_message_id, references=references,
        from_email=from_email,
    )
    raw = _encode_message(mime)

    result = api_call_with_retry(
        service.users().drafts().create(
            userId="me",
            body={"message": {"raw": raw, "threadId": thread_id}},
        ).execute
    )
    draft_id = result["id"]
    logger.info("Created draft reply thread=%s draft_id=%s", thread_id, draft_id)
    return draft_id


def modify_thread(thread_id, add_labels=None, remove_labels=None, account=None) -> dict:
    """Add/remove labels on a thread."""
    service = get_gmail_service(account)
    body = {}
    if add_labels:
        body["addLabelIds"] = add_labels
    if remove_labels:
        body["removeLabelIds"] = remove_labels
    return api_call_with_retry(
        service.users().threads().modify(userId="me", id=thread_id, body=body).execute
    )


def batch_modify_threads(modifications: list, rate_limit: int = 20, account=None) -> dict:
    """Modify multiple threads with rate limiting (serial, not batch).

    Gmail API limits: 15,000 quota units/min/user, threads.modify = 10 units
    = 1,500 ops/min max. We use 20/sec (1,200/min) for safety.

    Args:
        modifications: List of dicts with {thread_id, add_labels?, remove_labels?}
        rate_limit: Requests per second (default 20, max safe is 25)
        account: Account slug (optional, uses current context)

    Returns:
        {success: int, errors: int, error_ids: list}
    """
    service = get_gmail_service(account)
    stats = {"success": 0, "errors": 0, "error_ids": []}

    delay = 1.0 / rate_limit  # Time between requests

    for i, mod in enumerate(modifications):
        thread_id = mod["thread_id"]
        body = {}
        if mod.get("add_labels"):
            body["addLabelIds"] = mod["add_labels"]
        if mod.get("remove_labels"):
            body["removeLabelIds"] = mod["remove_labels"]

        try:
            api_call_with_retry(
                service.users().threads().modify(userId="me", id=thread_id, body=body).execute
            )
            stats["success"] += 1
        except HttpError as e:
            error_str = str(e)
            if "404" in error_str or "notFound" in error_str:
                # Thread deleted - count as success (nothing to do)
                stats["success"] += 1
            else:
                stats["errors"] += 1
                stats["error_ids"].append(thread_id)
                if stats["errors"] <= 5:
                    logger.warning("Modify thread %s failed: %s", thread_id, e)

        # Rate limiting delay
        if i < len(modifications) - 1:
            time.sleep(delay)

    return stats


def batch_get_threads(thread_ids, format="metadata", batch_size=50, account=None) -> list:
    """Bulk fetch threads. Returns list of thread dicts."""
    results = []
    for i in range(0, len(thread_ids), batch_size):
        chunk = thread_ids[i:i + batch_size]
        for tid in chunk:
            try:
                thread = get_thread(tid, format=format, account=account)
                results.append(thread)
            except HttpError as e:
                logger.warning("Failed to fetch thread %s: %s", tid, e)
                results.append({"id": tid, "error": str(e)})
        # Brief pause between batches to respect rate limits
        if i + batch_size < len(thread_ids):
            time.sleep(0.1)
    return results


# ---------------------------------------------------------------------------
# History (incremental sync)
# ---------------------------------------------------------------------------

def get_history(start_history_id, history_types=None, label_id=None, account=None) -> dict:
    """Incremental sync since last poll. Returns {history: [...], historyId: str}.

    Handles pagination internally — returns the complete history list.
    """
    service = get_gmail_service(account)
    if history_types is None:
        history_types = ["messagesAdded"]

    all_history = []
    page_token = None

    while True:
        kwargs = {
            "userId": "me",
            "startHistoryId": start_history_id,
            "historyTypes": history_types,
        }
        if label_id:
            kwargs["labelId"] = label_id
        if page_token:
            kwargs["pageToken"] = page_token

        resp = api_call_with_retry(
            service.users().history().list(**kwargs).execute
        )
        all_history.extend(resp.get("history", []))
        latest_id = resp.get("historyId", str(start_history_id))

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return {
        "history": all_history,
        "historyId": latest_id,
    }


def get_profile(account=None) -> dict:
    """Get account email + current history ID."""
    service = get_gmail_service(account)
    profile = api_call_with_retry(
        service.users().getProfile(userId="me").execute
    )
    return {
        "emailAddress": profile.get("emailAddress"),
        "historyId": profile.get("historyId"),
    }


# ---------------------------------------------------------------------------
# Labels
# ---------------------------------------------------------------------------

def create_label(name, account=None) -> dict:
    """Create a Gmail label. Returns label dict."""
    service = get_gmail_service(account)
    body = {
        "name": name,
        "labelListVisibility": "labelShow",
        "messageListVisibility": "show",
    }
    return api_call_with_retry(
        service.users().labels().create(userId="me", body=body).execute
    )


def patch_label(label_id, name=None, background_color=None, text_color=None, account=None) -> dict:
    """Update a Gmail label (rename, change color). Returns updated label dict.

    Args:
        label_id: Gmail label ID
        name: New label name (optional)
        background_color: Hex color for background, e.g. "#16a765" (optional)
        text_color: Hex color for text, e.g. "#ffffff" (optional)
        account: Account slug (optional, uses current context)
    """
    service = get_gmail_service(account)
    body = {}
    if name is not None:
        body["name"] = name
    if background_color or text_color:
        body["color"] = {}
        if background_color:
            body["color"]["backgroundColor"] = background_color
        if text_color:
            body["color"]["textColor"] = text_color
    return api_call_with_retry(
        service.users().labels().patch(userId="me", id=label_id, body=body).execute
    )


def delete_label(label_id, account=None) -> None:
    """Delete a Gmail label by ID."""
    service = get_gmail_service(account)
    api_call_with_retry(
        service.users().labels().delete(userId="me", id=label_id).execute
    )


def get_labels(account=None) -> list:
    """List all labels. Returns list of label dicts."""
    service = get_gmail_service(account)
    resp = api_call_with_retry(
        service.users().labels().list(userId="me").execute
    )
    return resp.get("labels", [])


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_message_headers(message) -> dict:
    """Extract headers to flat dict {From, To, Cc, Subject, Date, Message-ID, In-Reply-To, References}."""
    target_headers = {
        "From", "To", "Cc", "Subject", "Date",
        "Message-ID", "Message-Id",  # Gmail uses both casings
        "In-Reply-To", "References",
    }
    result = {}
    headers = message.get("payload", {}).get("headers", [])
    for h in headers:
        name = h.get("name", "")
        # Normalize Message-Id -> Message-ID
        key = "Message-ID" if name.lower() == "message-id" else name
        if name in target_headers or key in target_headers:
            result[key] = h.get("value", "")
    return result


def extract_body(message) -> dict:
    """Parse MIME, decode base64, return {text: str, html: str}.

    Handles multipart/alternative, multipart/mixed, nested structures.
    """
    text_parts = []
    html_parts = []

    def _walk_parts(payload):
        mime_type = payload.get("mimeType", "")
        body_data = payload.get("body", {}).get("data")

        if mime_type.startswith("multipart/"):
            for part in payload.get("parts", []):
                _walk_parts(part)
        elif body_data:
            decoded = _decode_base64(body_data)
            if mime_type == "text/plain":
                text_parts.append(decoded)
            elif mime_type == "text/html":
                html_parts.append(decoded)
            elif mime_type == "text/calendar":
                # Parse ICS calendar data into human-readable text
                ics_summary = _parse_ics_summary(decoded)
                if ics_summary:
                    text_parts.append(ics_summary)

    payload = message.get("payload", {})
    # Simple (non-multipart) message
    body_data = payload.get("body", {}).get("data")
    if body_data and not payload.get("parts"):
        decoded = _decode_base64(body_data)
        mime_type = payload.get("mimeType", "text/plain")
        if "html" in mime_type:
            return {"text": "", "html": decoded}
        return {"text": decoded, "html": ""}

    _walk_parts(payload)

    return {
        "text": "\n".join(text_parts),
        "html": "\n".join(html_parts),
    }


def _decode_base64(data: str) -> str:
    """Decode Gmail's URL-safe base64 encoded data."""
    # Add padding if needed
    padded = data + "=" * (4 - len(data) % 4) if len(data) % 4 else data
    decoded = base64.urlsafe_b64decode(padded).decode("utf-8", errors="replace")
    # Strip BOM, normalize line endings
    decoded = decoded.lstrip("\ufeff")
    decoded = decoded.replace("\r\n", "\n").replace("\r", "\n")
    return decoded


def _parse_ics_summary(ics_data: str) -> str:
    """Extract human-readable summary from ICS calendar data.

    Parses iCalendar (RFC 5545) format to extract:
    - SUMMARY: The meeting/event title
    - PARTSTAT: Participant status (ACCEPTED, DECLINED, TENTATIVE)
    - METHOD: Calendar action (REQUEST, REPLY, CANCEL)

    Returns a human-readable string like:
        "Calendar response: accepted - Weekly Team Sync"
    """
    lines = ics_data.replace("\r\n", "\n").split("\n")
    summary = ""
    status = ""
    method = ""

    for line in lines:
        # Handle line folding (continuation lines start with space/tab)
        line = line.strip()
        if not line:
            continue

        if line.startswith("SUMMARY:"):
            summary = line[8:].strip()
        elif line.startswith("METHOD:"):
            method = line[7:].strip().lower()
        elif "PARTSTAT=" in line:
            # ATTENDEE;PARTSTAT=ACCEPTED;CN=Name:mailto:email@example.com
            if "PARTSTAT=ACCEPTED" in line:
                status = "accepted"
            elif "PARTSTAT=DECLINED" in line:
                status = "declined"
            elif "PARTSTAT=TENTATIVE" in line:
                status = "tentative"
            elif "PARTSTAT=NEEDS-ACTION" in line:
                status = "pending"

    # Build human-readable output
    parts = []
    if method:
        if method == "reply":
            parts.append("Calendar response")
        elif method == "request":
            parts.append("Calendar invite")
        elif method == "cancel":
            parts.append("Calendar cancellation")
        else:
            parts.append(f"Calendar {method}")
    else:
        parts.append("Calendar event")

    if status:
        parts.append(status)
    if summary:
        parts.append(summary)

    return ": ".join(parts) if len(parts) > 1 else (parts[0] if parts else "")


def _extract_email_address(header_value: str) -> Optional[str]:
    """Extract bare email from a header like 'Name <email@example.com>'."""
    if not header_value:
        return None
    match = re.search(r"<([^>]+)>", header_value)
    if match:
        return match.group(1)
    # Might be bare email
    if "@" in header_value:
        return header_value.strip()
    return None


def is_our_email(email_address: str) -> bool:
    """Check if address is one of our 3 accounts."""
    if not email_address:
        return False
    addr = _extract_email_address(email_address)
    if addr:
        return addr.lower() in {e.lower() for e in OUR_EMAILS}
    return email_address.lower().strip() in {e.lower() for e in OUR_EMAILS}
