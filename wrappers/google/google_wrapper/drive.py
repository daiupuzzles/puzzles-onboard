"""
Google Drive Operations — Files, folders, permissions, comments, and file management.
All functions default to the current account context via _base.get_current_account().
"""

import io
import logging
import mimetypes
from typing import Optional, Union

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload, MediaIoBaseUpload

from ._base import (
    get_drive_service,
    get_current_account,
    api_call_with_retry,
)

logger = logging.getLogger("google_wrapper.drive")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GOOGLE_DOC = "application/vnd.google-apps.document"
GOOGLE_SHEET = "application/vnd.google-apps.spreadsheet"
GOOGLE_SLIDE = "application/vnd.google-apps.presentation"
GOOGLE_FOLDER = "application/vnd.google-apps.folder"

EXPORT_FORMATS = {
    "pdf": "application/pdf",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "txt": "text/plain",
    "html": "text/html",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "csv": "text/csv",
    "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "md": "text/markdown",
    "odt": "application/vnd.oasis.opendocument.text",
    "rtf": "application/rtf",
    "epub": "application/epub+zip",
}

DEFAULT_FILE_FIELDS = (
    "id,name,mimeType,size,modifiedTime,createdTime,"
    "parents,webViewLink,webContentLink,trashed,shared,description"
)


# ---------------------------------------------------------------------------
# File Operations
# ---------------------------------------------------------------------------

def list_files(query=None, folder_id=None, max_results=100, fields=None,
               order_by="modifiedTime desc", trashed=False, account=None) -> list:
    """List files with optional Drive query. Handles pagination internally.

    Args:
        query: Raw Drive query string (e.g., "name contains 'report'")
        folder_id: Limit to files in this folder
        max_results: Maximum files to return (default 100)
        fields: Comma-separated file fields (default: DEFAULT_FILE_FIELDS)
        order_by: Sort order (default: modifiedTime desc)
        trashed: Include trashed files (default: False)
        account: Account slug (optional)

    Returns:
        List of file metadata dicts.
    """
    service = get_drive_service(account)
    file_fields = fields or DEFAULT_FILE_FIELDS

    # Build query
    parts = []
    if query:
        parts.append(query)
    if folder_id:
        parts.append(f"'{folder_id}' in parents")
    if not trashed:
        parts.append("trashed = false")
    q = " and ".join(parts) if parts else None

    files = []
    page_token = None

    while len(files) < max_results:
        kwargs = {
            "pageSize": min(max_results - len(files), 1000),
            "fields": f"nextPageToken, files({file_fields})",
        }
        if q:
            kwargs["q"] = q
        if order_by:
            kwargs["orderBy"] = order_by
        if page_token:
            kwargs["pageToken"] = page_token

        resp = api_call_with_retry(
            service.files().list(**kwargs).execute
        )
        batch = resp.get("files", [])
        files.extend(batch)

        page_token = resp.get("nextPageToken")
        if not page_token or not batch:
            break

    return files[:max_results]


def search_files(name_contains=None, mime_type=None, full_text=None,
                 folder_id=None, max_results=50, account=None) -> list:
    """High-level file search with user-friendly parameters.

    Args:
        name_contains: File name contains this string
        mime_type: Filter by MIME type (e.g., "application/pdf" or GOOGLE_DOC)
        full_text: Full-text search across file content
        folder_id: Limit to files in this folder
        max_results: Maximum files to return (default 50)
        account: Account slug (optional)

    Returns:
        List of file metadata dicts.
    """
    parts = []
    if name_contains:
        parts.append(f"name contains '{name_contains}'")
    if mime_type:
        parts.append(f"mimeType = '{mime_type}'")
    if full_text:
        parts.append(f"fullText contains '{full_text}'")

    query = " and ".join(parts) if parts else None
    return list_files(query=query, folder_id=folder_id, max_results=max_results, account=account)


def get_file_metadata(file_id, fields=None, account=None) -> dict:
    """Get full metadata for a single file.

    Returns dict with id, name, mimeType, size, modifiedTime, parents, webViewLink, etc.
    """
    service = get_drive_service(account)
    file_fields = fields or DEFAULT_FILE_FIELDS
    return api_call_with_retry(
        service.files().get(fileId=file_id, fields=file_fields).execute
    )


def download_file(file_id, destination_path, account=None) -> str:
    """Download a binary (non-Google-native) file to local path.

    For Google Docs/Sheets/Slides, use export_file() instead.

    Returns:
        The destination path.
    """
    service = get_drive_service(account)
    request = service.files().get_media(fileId=file_id)

    with open(destination_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, request, chunksize=50 * 1024 * 1024)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                logger.debug("Download %s: %d%%", file_id, int(status.progress() * 100))

    logger.info("Downloaded file %s to %s", file_id, destination_path)
    return destination_path


def export_file(file_id, mime_type, destination_path=None, account=None) -> Union[str, bytes]:
    """Export a Google-native file (Doc, Sheet, Slide) to a target format.

    Args:
        file_id: The Google file ID
        mime_type: Target format — short name ("pdf", "docx", "txt", "html", "xlsx",
                   "csv", "pptx", "md") or full MIME type
        destination_path: If provided, writes to disk and returns path. Otherwise returns bytes.
        account: Account slug (optional)

    Returns:
        File path (if destination_path given) or bytes content.
    """
    service = get_drive_service(account)

    # Resolve short names
    resolved = EXPORT_FORMATS.get(mime_type, mime_type)

    request = service.files().export_media(fileId=file_id, mimeType=resolved)
    content = api_call_with_retry(request.execute)

    if destination_path:
        with open(destination_path, "wb") as f:
            f.write(content)
        logger.info("Exported file %s as %s to %s", file_id, resolved, destination_path)
        return destination_path

    return content


def upload_file(local_path, name=None, folder_id=None, mime_type=None,
                convert_to=None, description=None, account=None) -> dict:
    """Upload a local file to Drive.

    Args:
        local_path: Path to the local file
        name: File name in Drive (default: local filename)
        folder_id: Destination folder ID (default: root)
        mime_type: MIME type of the file (auto-detected if not provided)
        convert_to: Google Workspace MIME type to convert to (e.g., GOOGLE_DOC)
        description: File description
        account: Account slug (optional)

    Returns:
        File metadata dict (id, name, mimeType, webViewLink, etc.)
    """
    service = get_drive_service(account)

    if not mime_type:
        mime_type, _ = mimetypes.guess_type(local_path)
        mime_type = mime_type or "application/octet-stream"

    file_name = name or local_path.rsplit("/", 1)[-1]

    file_metadata = {"name": file_name}
    if folder_id:
        file_metadata["parents"] = [folder_id]
    if convert_to:
        file_metadata["mimeType"] = convert_to
    if description:
        file_metadata["description"] = description

    media = MediaFileUpload(local_path, mimetype=mime_type, resumable=True)

    result = api_call_with_retry(
        service.files().create(
            body=file_metadata,
            media_body=media,
            fields=DEFAULT_FILE_FIELDS,
        ).execute
    )
    logger.info("Uploaded %s as %s (id=%s)", local_path, file_name, result["id"])
    return result


def update_file_content(file_id, local_path, mime_type=None, account=None) -> dict:
    """Replace the content of an existing file (creates new revision).

    Returns:
        Updated file metadata dict.
    """
    service = get_drive_service(account)

    if not mime_type:
        mime_type, _ = mimetypes.guess_type(local_path)
        mime_type = mime_type or "application/octet-stream"

    media = MediaFileUpload(local_path, mimetype=mime_type, resumable=True)
    result = api_call_with_retry(
        service.files().update(
            fileId=file_id,
            media_body=media,
            fields=DEFAULT_FILE_FIELDS,
        ).execute
    )
    logger.info("Updated content of file %s from %s", file_id, local_path)
    return result


def copy_file(file_id, new_name=None, folder_id=None, account=None) -> dict:
    """Copy a file. Optionally rename and place in a different folder.

    Returns:
        New file metadata dict.
    """
    service = get_drive_service(account)
    body = {}
    if new_name:
        body["name"] = new_name
    if folder_id:
        body["parents"] = [folder_id]

    result = api_call_with_retry(
        service.files().copy(fileId=file_id, body=body, fields=DEFAULT_FILE_FIELDS).execute
    )
    logger.info("Copied file %s -> %s", file_id, result["id"])
    return result


# ---------------------------------------------------------------------------
# Folder Operations
# ---------------------------------------------------------------------------

def create_folder(name, parent_folder_id=None, account=None) -> dict:
    """Create a folder in Drive.

    Returns:
        Folder metadata dict (id, name, webViewLink).
    """
    service = get_drive_service(account)
    metadata = {
        "name": name,
        "mimeType": GOOGLE_FOLDER,
    }
    if parent_folder_id:
        metadata["parents"] = [parent_folder_id]

    result = api_call_with_retry(
        service.files().create(
            body=metadata,
            fields="id,name,webViewLink",
        ).execute
    )
    logger.info("Created folder '%s' (id=%s)", name, result["id"])
    return result


def list_folder_contents(folder_id, max_results=100, account=None) -> list:
    """List files in a specific folder.

    Returns:
        List of file metadata dicts.
    """
    return list_files(folder_id=folder_id, max_results=max_results, account=account)


def move_file(file_id, new_parent_id, account=None) -> dict:
    """Move a file or folder to a new parent folder.

    Returns:
        Updated file metadata dict.
    """
    service = get_drive_service(account)

    # Get current parents to remove
    current = api_call_with_retry(
        service.files().get(fileId=file_id, fields="parents").execute
    )
    previous_parents = ",".join(current.get("parents", []))

    result = api_call_with_retry(
        service.files().update(
            fileId=file_id,
            addParents=new_parent_id,
            removeParents=previous_parents,
            fields=DEFAULT_FILE_FIELDS,
        ).execute
    )
    logger.info("Moved file %s to folder %s", file_id, new_parent_id)
    return result


def get_or_create_folder_path(path, root_folder_id="root", account=None) -> str:
    """Create nested folder path, creating each level if it doesn't exist.

    Example:
        folder_id = get_or_create_folder_path("Projects/2026/Q1 Reports")

    Args:
        path: Forward-slash-separated folder path (e.g., "A/B/C")
        root_folder_id: Starting parent folder (default: Drive root)
        account: Account slug (optional)

    Returns:
        The ID of the leaf (deepest) folder.
    """
    service = get_drive_service(account)
    current_parent = root_folder_id

    for folder_name in path.split("/"):
        folder_name = folder_name.strip()
        if not folder_name:
            continue

        # Search for existing folder at this level
        q = (
            f"name = '{folder_name}' and "
            f"'{current_parent}' in parents and "
            f"mimeType = '{GOOGLE_FOLDER}' and "
            f"trashed = false"
        )
        resp = api_call_with_retry(
            service.files().list(q=q, fields="files(id,name)", pageSize=1).execute
        )
        existing = resp.get("files", [])

        if existing:
            current_parent = existing[0]["id"]
        else:
            folder = create_folder(folder_name, parent_folder_id=current_parent, account=account)
            current_parent = folder["id"]

    return current_parent


# ---------------------------------------------------------------------------
# File Management
# ---------------------------------------------------------------------------

def trash_file(file_id, account=None) -> dict:
    """Move a file to trash.

    Returns:
        Updated file metadata dict.
    """
    service = get_drive_service(account)
    result = api_call_with_retry(
        service.files().update(fileId=file_id, body={"trashed": True}, fields=DEFAULT_FILE_FIELDS).execute
    )
    logger.info("Trashed file %s", file_id)
    return result


def delete_file_permanently(file_id, account=None) -> None:
    """Permanently delete a file (no recovery)."""
    service = get_drive_service(account)
    api_call_with_retry(
        service.files().delete(fileId=file_id).execute
    )
    logger.info("Permanently deleted file %s", file_id)


def rename_file(file_id, new_name, account=None) -> dict:
    """Rename a file.

    Returns:
        Updated file metadata dict.
    """
    service = get_drive_service(account)
    result = api_call_with_retry(
        service.files().update(fileId=file_id, body={"name": new_name}, fields=DEFAULT_FILE_FIELDS).execute
    )
    logger.info("Renamed file %s to '%s'", file_id, new_name)
    return result


# ---------------------------------------------------------------------------
# Permissions and Sharing
# ---------------------------------------------------------------------------

def share_file(file_id, email, role="reader", send_notification=True,
               message=None, account=None) -> dict:
    """Share a file with a user.

    Args:
        file_id: The file ID
        email: Recipient email address
        role: "reader", "commenter", "writer", or "owner"
        send_notification: Send email notification (default: True)
        message: Custom message in the notification email
        account: Account slug (optional)

    Returns:
        Permission dict.
    """
    service = get_drive_service(account)
    permission = {
        "type": "user",
        "role": role,
        "emailAddress": email,
    }
    kwargs = {
        "fileId": file_id,
        "body": permission,
        "sendNotificationEmail": send_notification,
    }
    if message:
        kwargs["emailMessage"] = message

    result = api_call_with_retry(
        service.permissions().create(**kwargs).execute
    )
    logger.info("Shared file %s with %s (role=%s)", file_id, email, role)
    return result


def share_file_with_link(file_id, role="reader", account=None) -> str:
    """Make a file accessible via shareable link.

    Args:
        file_id: The file ID
        role: "reader", "commenter", or "writer"
        account: Account slug (optional)

    Returns:
        The shareable link URL.
    """
    service = get_drive_service(account)
    permission = {
        "type": "anyone",
        "role": role,
    }
    api_call_with_retry(
        service.permissions().create(fileId=file_id, body=permission).execute
    )

    # Get the file's webViewLink
    file_meta = api_call_with_retry(
        service.files().get(fileId=file_id, fields="webViewLink").execute
    )
    link = file_meta.get("webViewLink", "")
    logger.info("Created shareable link for file %s (role=%s)", file_id, role)
    return link


def list_permissions(file_id, account=None) -> list:
    """List all permissions on a file.

    Returns:
        List of permission dicts.
    """
    service = get_drive_service(account)
    resp = api_call_with_retry(
        service.permissions().list(
            fileId=file_id,
            fields="permissions(id,type,role,emailAddress,displayName)",
        ).execute
    )
    return resp.get("permissions", [])


def remove_permission(file_id, permission_id, account=None) -> None:
    """Remove a permission by ID."""
    service = get_drive_service(account)
    api_call_with_retry(
        service.permissions().delete(fileId=file_id, permissionId=permission_id).execute
    )
    logger.info("Removed permission %s from file %s", permission_id, file_id)


# ---------------------------------------------------------------------------
# Comments
# ---------------------------------------------------------------------------

def list_comments(file_id, max_results=100, include_deleted=False, account=None) -> list:
    """List comments on a file. Handles pagination.

    Returns:
        List of comment dicts.
    """
    service = get_drive_service(account)
    comments = []
    page_token = None

    while len(comments) < max_results:
        kwargs = {
            "fileId": file_id,
            "pageSize": min(max_results - len(comments), 100),
            "fields": "nextPageToken, comments(id,content,author,createdTime,modifiedTime,resolved,replies)",
            "includeDeleted": include_deleted,
        }
        if page_token:
            kwargs["pageToken"] = page_token

        resp = api_call_with_retry(
            service.comments().list(**kwargs).execute
        )
        batch = resp.get("comments", [])
        comments.extend(batch)

        page_token = resp.get("nextPageToken")
        if not page_token or not batch:
            break

    return comments[:max_results]


def add_comment(file_id, content, anchor=None, account=None) -> dict:
    """Add a comment to a file.

    Args:
        file_id: The file ID
        content: Comment text
        anchor: Optional anchor for position-specific comments (Drive API anchor format)
        account: Account slug (optional)

    Returns:
        Comment dict.
    """
    service = get_drive_service(account)
    body = {"content": content}
    if anchor:
        body["anchor"] = anchor

    result = api_call_with_retry(
        service.comments().create(
            fileId=file_id,
            body=body,
            fields="id,content,author,createdTime",
        ).execute
    )
    logger.info("Added comment to file %s", file_id)
    return result


def reply_to_comment(file_id, comment_id, content, account=None) -> dict:
    """Reply to an existing comment.

    Returns:
        Reply dict.
    """
    service = get_drive_service(account)
    result = api_call_with_retry(
        service.replies().create(
            fileId=file_id,
            commentId=comment_id,
            body={"content": content},
            fields="id,content,author,createdTime",
        ).execute
    )
    logger.info("Replied to comment %s on file %s", comment_id, file_id)
    return result


def resolve_comment(file_id, comment_id, account=None) -> dict:
    """Mark a comment as resolved.

    Returns:
        Updated comment dict.
    """
    service = get_drive_service(account)
    result = api_call_with_retry(
        service.comments().update(
            fileId=file_id,
            commentId=comment_id,
            body={"resolved": True},
            fields="id,content,resolved",
        ).execute
    )
    logger.info("Resolved comment %s on file %s", comment_id, file_id)
    return result
