"""
Google Docs Operations — Read, create, edit, and format documents.
All functions default to the current account context via _base.get_current_account().
"""

import io
import logging
import re
from typing import Optional

from googleapiclient.errors import HttpError

from ._base import (
    get_docs_service,
    get_drive_service,
    get_current_account,
    api_call_with_retry,
)

logger = logging.getLogger("google_wrapper.docs")


# ---------------------------------------------------------------------------
# Internal Helpers
# ---------------------------------------------------------------------------

def _get_end_index(document) -> int:
    """Get the end index of the document body (last valid insertion point).

    The Docs API body has structural elements, each with a startIndex and endIndex.
    The last element's endIndex - 1 is the last valid insertion point.
    """
    body = document.get("body", {})
    content = body.get("content", [])
    if not content:
        return 1
    last_element = content[-1]
    return last_element.get("endIndex", 1) - 1


def _extract_text_from_element(element) -> str:
    """Extract text from a structural element (paragraph, table, etc.)."""
    text_parts = []

    # Paragraph
    paragraph = element.get("paragraph")
    if paragraph:
        for elem in paragraph.get("elements", []):
            text_run = elem.get("textRun")
            if text_run:
                text_parts.append(text_run.get("content", ""))
        return "".join(text_parts)

    # Table — extract text from all cells
    table = element.get("table")
    if table:
        for row in table.get("tableRows", []):
            for cell in row.get("tableCells", []):
                for cell_element in cell.get("content", []):
                    text_parts.append(_extract_text_from_element(cell_element))
        return "".join(text_parts)

    # Table of contents
    toc = element.get("tableOfContents")
    if toc:
        for toc_element in toc.get("content", []):
            text_parts.append(_extract_text_from_element(toc_element))
        return "".join(text_parts)

    return ""


# ---------------------------------------------------------------------------
# Document CRUD
# ---------------------------------------------------------------------------

def create_doc(title, folder_id=None, body_text=None, account=None) -> dict:
    """Create a new Google Doc.

    Args:
        title: Document title
        folder_id: Optional folder to place the doc in (uses Drive API to move after creation)
        body_text: Optional initial text to insert
        account: Account slug (optional)

    Returns:
        Dict with documentId, title, and webViewLink.
    """
    docs_service = get_docs_service(account)

    doc = api_call_with_retry(
        docs_service.documents().create(body={"title": title}).execute
    )
    doc_id = doc["documentId"]
    logger.info("Created doc '%s' (id=%s)", title, doc_id)

    # Insert initial text if provided
    if body_text:
        append_text(doc_id, body_text, account=account)

    # Move to folder if specified (Docs API doesn't support parents)
    if folder_id:
        drive_service = get_drive_service(account)
        api_call_with_retry(
            drive_service.files().update(
                fileId=doc_id,
                addParents=folder_id,
                removeParents="root",
                fields="id,parents",
            ).execute
        )
        logger.info("Moved doc %s to folder %s", doc_id, folder_id)

    # Get webViewLink
    drive_service = get_drive_service(account)
    meta = api_call_with_retry(
        drive_service.files().get(fileId=doc_id, fields="webViewLink").execute
    )

    return {
        "documentId": doc_id,
        "title": title,
        "webViewLink": meta.get("webViewLink", ""),
    }


def get_doc(document_id, account=None) -> dict:
    """Get full document structure.

    Returns the raw Docs API document object with title, body, headers, footers,
    lists, tables, and all structural elements.
    """
    service = get_docs_service(account)
    return api_call_with_retry(
        service.documents().get(documentId=document_id).execute
    )


def get_doc_text(document_id, account=None) -> str:
    """Extract all text content from a Doc as a plain string.

    Walks the document body's structural elements and concatenates
    paragraph text runs and table cell content.
    """
    doc = get_doc(document_id, account=account)
    body = doc.get("body", {})
    content = body.get("content", [])

    text_parts = []
    for element in content:
        text_parts.append(_extract_text_from_element(element))

    return "".join(text_parts)


def get_doc_metadata(document_id, account=None) -> dict:
    """Get document title, revision ID, and suggestions view mode.

    Lighter than get_doc() when you only need metadata.
    """
    service = get_docs_service(account)
    doc = api_call_with_retry(
        service.documents().get(documentId=document_id).execute
    )
    return {
        "documentId": doc.get("documentId"),
        "title": doc.get("title"),
        "revisionId": doc.get("revisionId"),
        "suggestionsViewMode": doc.get("suggestionsViewMode"),
    }


# ---------------------------------------------------------------------------
# Content Editing
# ---------------------------------------------------------------------------

def append_text(document_id, text, account=None) -> dict:
    """Append text to the end of a document.

    Reads the document to find the end index, then inserts text there.

    Returns:
        Batch update response.
    """
    doc = get_doc(document_id, account=account)
    end_index = _get_end_index(doc)

    # If end_index is 1, the doc is empty — insert at index 1
    insert_index = max(end_index, 1)

    requests = [
        {
            "insertText": {
                "location": {"index": insert_index},
                "text": text,
            }
        }
    ]
    return batch_update(document_id, requests, account=account)


def insert_text(document_id, text, index=1, account=None) -> dict:
    """Insert text at a specific character index.

    Index 1 = beginning of body. Use get_doc() to inspect structure and find indices.

    Returns:
        Batch update response.
    """
    requests = [
        {
            "insertText": {
                "location": {"index": index},
                "text": text,
            }
        }
    ]
    return batch_update(document_id, requests, account=account)


def replace_text(document_id, find_text, replace_with, match_case=True, account=None) -> dict:
    """Find-and-replace all occurrences of a string in the document.

    Args:
        document_id: The document ID
        find_text: Text to find
        replace_with: Replacement text
        match_case: Case-sensitive matching (default: True)
        account: Account slug (optional)

    Returns:
        Batch update response with replaceAllText reply (includes occurrencesChanged count).
    """
    requests = [
        {
            "replaceAllText": {
                "containsText": {
                    "text": find_text,
                    "matchCase": match_case,
                },
                "replaceText": replace_with,
            }
        }
    ]
    return batch_update(document_id, requests, account=account)


def delete_range(document_id, start_index, end_index, account=None) -> dict:
    """Delete content between two character indices.

    Returns:
        Batch update response.
    """
    requests = [
        {
            "deleteContentRange": {
                "range": {
                    "startIndex": start_index,
                    "endIndex": end_index,
                }
            }
        }
    ]
    return batch_update(document_id, requests, account=account)


def batch_update(document_id, requests, account=None) -> dict:
    """Execute a list of raw Docs API requests.

    This is the power-user escape hatch for any operation the higher-level
    functions don't cover (e.g., inserting inline images, updating table cells,
    creating headers/footers).

    Args:
        document_id: The document ID
        requests: List of Docs API request dicts
        account: Account slug (optional)

    Returns:
        Full batch update response dict.
    """
    service = get_docs_service(account)
    result = api_call_with_retry(
        service.documents().batchUpdate(
            documentId=document_id,
            body={"requests": requests},
        ).execute
    )
    logger.debug("Batch update on doc %s: %d requests", document_id, len(requests))
    return result


def clear_doc(document_id, account=None) -> dict:
    """Delete all body content, leaving an empty document.

    Returns:
        Batch update response.
    """
    doc = get_doc(document_id, account=account)
    end_index = _get_end_index(doc)

    if end_index <= 1:
        logger.debug("Doc %s is already empty", document_id)
        return {"replies": []}

    return delete_range(document_id, 1, end_index, account=account)


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def apply_text_style(document_id, start_index, end_index,
                     bold=None, italic=None, underline=None,
                     font_size=None, font_family=None,
                     foreground_color=None, link_url=None,
                     account=None) -> dict:
    """Apply text formatting to a character range.

    Only non-None parameters are applied. All others remain unchanged.

    Args:
        document_id: The document ID
        start_index: Start of range (1-based)
        end_index: End of range
        bold: True/False
        italic: True/False
        underline: True/False
        font_size: Font size in points (e.g., 12, 14, 18)
        font_family: Font name (e.g., "Arial", "Roboto", "Courier New")
        foreground_color: Text color as dict {red, green, blue} with 0-1 floats,
                          or hex string like "#FF0000"
        link_url: URL to link the text to
        account: Account slug (optional)

    Returns:
        Batch update response.
    """
    style = {}
    fields = []

    if bold is not None:
        style["bold"] = bold
        fields.append("bold")
    if italic is not None:
        style["italic"] = italic
        fields.append("italic")
    if underline is not None:
        style["underline"] = underline
        fields.append("underline")
    if font_size is not None:
        style["fontSize"] = {"magnitude": font_size, "unit": "PT"}
        fields.append("fontSize")
    if font_family is not None:
        style["weightedFontFamily"] = {"fontFamily": font_family}
        fields.append("weightedFontFamily")
    if foreground_color is not None:
        if isinstance(foreground_color, str) and foreground_color.startswith("#"):
            # Convert hex to RGB floats
            hex_color = foreground_color.lstrip("#")
            r = int(hex_color[0:2], 16) / 255.0
            g = int(hex_color[2:4], 16) / 255.0
            b = int(hex_color[4:6], 16) / 255.0
            foreground_color = {"red": r, "green": g, "blue": b}
        style["foregroundColor"] = {"color": {"rgbColor": foreground_color}}
        fields.append("foregroundColor")
    if link_url is not None:
        style["link"] = {"url": link_url}
        fields.append("link")

    if not fields:
        logger.warning("apply_text_style called with no style parameters")
        return {"replies": []}

    requests = [
        {
            "updateTextStyle": {
                "range": {
                    "startIndex": start_index,
                    "endIndex": end_index,
                },
                "textStyle": style,
                "fields": ",".join(fields),
            }
        }
    ]
    return batch_update(document_id, requests, account=account)


def apply_paragraph_style(document_id, start_index, end_index,
                          named_style=None, alignment=None,
                          indent_start=None, spacing_before=None,
                          spacing_after=None, account=None) -> dict:
    """Apply paragraph-level formatting to a range.

    Args:
        document_id: The document ID
        start_index: Start of range (1-based)
        end_index: End of range
        named_style: "HEADING_1" through "HEADING_6", "TITLE", "SUBTITLE", "NORMAL_TEXT"
        alignment: "START", "CENTER", "END", "JUSTIFIED"
        indent_start: Left indent in points
        spacing_before: Space before paragraph in points
        spacing_after: Space after paragraph in points
        account: Account slug (optional)

    Returns:
        Batch update response.
    """
    style = {}
    fields = []

    if named_style is not None:
        style["namedStyleType"] = named_style
        fields.append("namedStyleType")
    if alignment is not None:
        style["alignment"] = alignment
        fields.append("alignment")
    if indent_start is not None:
        style["indentStart"] = {"magnitude": indent_start, "unit": "PT"}
        fields.append("indentStart")
    if spacing_before is not None:
        style["spaceAbove"] = {"magnitude": spacing_before, "unit": "PT"}
        fields.append("spaceAbove")
    if spacing_after is not None:
        style["spaceBelow"] = {"magnitude": spacing_after, "unit": "PT"}
        fields.append("spaceBelow")

    if not fields:
        logger.warning("apply_paragraph_style called with no style parameters")
        return {"replies": []}

    requests = [
        {
            "updateParagraphStyle": {
                "range": {
                    "startIndex": start_index,
                    "endIndex": end_index,
                },
                "paragraphStyle": style,
                "fields": ",".join(fields),
            }
        }
    ]
    return batch_update(document_id, requests, account=account)


# ---------------------------------------------------------------------------
# Structural Elements
# ---------------------------------------------------------------------------

def insert_page_break(document_id, index, account=None) -> dict:
    """Insert a page break at the specified index.

    Returns:
        Batch update response.
    """
    requests = [
        {
            "insertPageBreak": {
                "location": {"index": index},
            }
        }
    ]
    return batch_update(document_id, requests, account=account)


def insert_table(document_id, rows, columns, index=None, account=None) -> dict:
    """Insert a table at the specified index.

    Args:
        document_id: The document ID
        rows: Number of rows
        columns: Number of columns
        index: Insertion index (default: end of document)
        account: Account slug (optional)

    Returns:
        Batch update response.
    """
    if index is None:
        doc = get_doc(document_id, account=account)
        index = _get_end_index(doc)

    requests = [
        {
            "insertTable": {
                "rows": rows,
                "columns": columns,
                "location": {"index": index},
            }
        }
    ]
    return batch_update(document_id, requests, account=account)


# ---------------------------------------------------------------------------
# Markdown → Google Doc
# ---------------------------------------------------------------------------

def _md_to_html(md_text):
    """Convert markdown text to HTML suitable for Google Docs import.

    Handles: headings (#-######), links ([text](url)), bold (**), italic (*),
    inline code (`), tables (|...|), checkboxes (- [ ]), list items (- ), horizontal rules (---).
    """
    lines = md_text.split('\n')
    html = ['''<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
body { font-family: Arial, sans-serif; font-size: 11pt; line-height: 1.6; color: #1a1a1a; }
h1 { font-size: 20pt; border-bottom: 2px solid #333; padding-bottom: 4pt; margin-top: 28pt; }
h2 { font-size: 16pt; margin-top: 24pt; }
h3 { font-size: 13pt; margin-top: 18pt; color: #333; }
h4 { font-size: 11pt; margin-top: 14pt; color: #555; }
table { border-collapse: collapse; width: 100%; margin: 10pt 0; font-size: 10pt; }
th { background-color: #f2f2f2; font-weight: bold; text-align: left; padding: 6pt 8pt; border: 1pt solid #ccc; }
td { padding: 6pt 8pt; border: 1pt solid #ccc; vertical-align: top; }
hr { border: none; border-top: 2px solid #ddd; margin: 20pt 0; }
ul { margin: 4pt 0 4pt 20pt; }
li { margin: 2pt 0; }
</style></head><body>''']

    def _inline(text):
        """Process inline markdown: [links](url), **bold**, *italic*, `code`."""
        text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2">\1</a>', text)
        text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)
        text = re.sub(r'(?<!\*)\*([^*]+?)\*(?!\*)', r'<i>\1</i>', text)
        text = re.sub(r'`(.+?)`', r'<code>\1</code>', text)
        return text

    def _build_table(tbl_lines):
        """Convert markdown table lines to HTML table."""
        rows = []
        for tl in tbl_lines:
            stripped = tl.strip()
            if re.match(r'^\|[\s\-:|]+\|$', stripped):
                continue
            cells = [c.strip() for c in stripped.strip('|').split('|')]
            rows.append(cells)
        if not rows:
            return ''
        parts = ['<table>', '<tr>']
        for cell in rows[0]:
            parts.append(f'<th>{_inline(cell)}</th>')
        parts.append('</tr>')
        for row in rows[1:]:
            parts.append('<tr>')
            for cell in row:
                parts.append(f'<td>{_inline(cell)}</td>')
            parts.append('</tr>')
        parts.append('</table>')
        return '\n'.join(parts)

    i = 0
    while i < len(lines):
        line = lines[i]

        if not line.strip():
            i += 1
            continue

        # Horizontal rule
        if re.match(r'^---+\s*$', line):
            html.append('<hr>')
            i += 1
            continue

        # Heading
        m = re.match(r'^(#{1,6})\s+(.+)$', line)
        if m:
            level = len(m.group(1))
            text = _inline(m.group(2))
            html.append(f'<h{level}>{text}</h{level}>')
            i += 1
            continue

        # Table block
        if line.strip().startswith('|') and line.count('|') >= 2:
            table_lines_block = []
            while i < len(lines) and lines[i].strip().startswith('|'):
                table_lines_block.append(lines[i])
                i += 1
            html.append(_build_table(table_lines_block))
            continue

        # Checkbox item
        m = re.match(r'^- \[([ x])\] (.+)$', line)
        if m:
            checked = m.group(1) == 'x'
            symbol = '&#9745;' if checked else '&#9744;'
            text = _inline(m.group(2))
            html.append(f'<p>{symbol} {text}</p>')
            i += 1
            sub_items = []
            while i < len(lines) and re.match(r'^  +- (.+)$', lines[i]):
                sub_m = re.match(r'^  +- (.+)$', lines[i])
                sub_items.append(_inline(sub_m.group(1)))
                i += 1
            if sub_items:
                html.append('<ul>')
                for si in sub_items:
                    html.append(f'<li>{si}</li>')
                html.append('</ul>')
            continue

        # Regular list item
        m = re.match(r'^- (.+)$', line)
        if m:
            items = []
            while i < len(lines) and re.match(r'^- (.+)$', lines[i]):
                lm = re.match(r'^- (.+)$', lines[i])
                items.append(_inline(lm.group(1)))
                i += 1
            html.append('<ul>')
            for item in items:
                html.append(f'<li>{item}</li>')
            html.append('</ul>')
            continue

        # Blockquote
        if line.strip().startswith('>'):
            bq_lines = []
            while i < len(lines) and lines[i].strip().startswith('>'):
                bq_lines.append(re.sub(r'^>\s?', '', lines[i]))
                i += 1
            bq_html = ' '.join(_inline(bl.strip()) for bl in bq_lines if bl.strip())
            html.append(f'<blockquote style="border-left: 3pt solid #ccc; padding: 4pt 12pt; margin: 10pt 0; color: #555;">{bq_html}</blockquote>')
            continue

        # Normal paragraph
        html.append(f'<p>{_inline(line)}</p>')
        i += 1

    html.append('</body></html>')
    return '\n'.join(html)


def push_markdown(markdown_text, title, folder_id=None, doc_id=None, account=None) -> dict:
    """Convert markdown text to a formatted Google Doc.

    Converts markdown to HTML (headings, tables, bold, checkboxes, lists),
    then uploads via Drive API with automatic conversion to Google Docs format.

    If doc_id is provided, updates the existing document in place (replaces
    all content). Otherwise creates a new document.

    Args:
        markdown_text: Markdown content as a string
        title: Google Doc title
        folder_id: Optional Drive folder ID to place the document in (create only)
        doc_id: Optional existing document ID to update in place
        account: Account slug (optional)

    Returns:
        Dict with documentId, title, and webViewLink.
    """
    from googleapiclient.http import MediaIoBaseUpload

    html = _md_to_html(markdown_text)

    drive_service = get_drive_service(account)
    media = MediaIoBaseUpload(
        io.BytesIO(html.encode('utf-8')),
        mimetype='text/html',
        resumable=True,
    )

    is_update = doc_id is not None

    if is_update:
        # Update existing document in place
        file_metadata = {'name': title}
        result = api_call_with_retry(
            drive_service.files().update(
                fileId=doc_id,
                body=file_metadata,
                media_body=media,
                fields='id,webViewLink',
            ).execute
        )
        logger.info("Updated formatted doc '%s' (id=%s) from markdown", title, doc_id)
    else:
        # Create new document
        file_metadata = {
            'name': title,
            'mimeType': 'application/vnd.google-apps.document',
        }
        result = api_call_with_retry(
            drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id,webViewLink',
            ).execute
        )
        doc_id = result['id']
        logger.info("Created formatted doc '%s' (id=%s) from markdown", title, doc_id)

    link = result.get('webViewLink', f'https://docs.google.com/document/d/{doc_id}/edit')

    # Move to folder if specified (create only — updates stay where they are)
    if folder_id and not is_update:
        api_call_with_retry(
            drive_service.files().update(
                fileId=doc_id,
                addParents=folder_id,
                removeParents='root',
                fields='id,parents',
            ).execute
        )
        logger.info("Moved doc %s to folder %s", doc_id, folder_id)

    return {
        'documentId': doc_id,
        'title': title,
        'webViewLink': link,
    }


def push_markdown_file(file_path, title=None, folder_id=None, doc_id=None, account=None) -> dict:
    """Convert a markdown file to a formatted Google Doc.

    Convenience wrapper around push_markdown() that reads from a file path.

    Args:
        file_path: Path to the .md file
        title: Google Doc title (defaults to filename without extension)
        folder_id: Optional Drive folder ID (create only)
        doc_id: Optional existing document ID to update in place
        account: Account slug (optional)

    Returns:
        Dict with documentId, title, and webViewLink.
    """
    import os

    with open(file_path, 'r', encoding='utf-8') as f:
        md_text = f.read()

    if title is None:
        title = os.path.splitext(os.path.basename(file_path))[0].replace('-', ' ').replace('_', ' ').title()

    return push_markdown(md_text, title, folder_id=folder_id, doc_id=doc_id, account=account)
