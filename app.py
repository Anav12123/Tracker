from flask import Flask, send_file
from datetime import datetime, timezone
import base64
import json
import os
import io
import pytz
import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)

# === CONFIG ===

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
IST                   = pytz.timezone("Asia/Kolkata")
MAILTRACKING_WORKBOOK = "MailTracking"

# Transparent 1×1 GIF payload
PIXEL_BYTES = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00"
    b"\xFF\xFF\xFF!\xF9\x04\x01\x00\x00\x00\x00,"
    b"\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02"
    b"L\x01\x00;"
)

# === Google Sheets client ===

creds_info = json.loads(os.environ["GOOGLE_CREDS_JSON"])
creds      = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
gc         = gspread.authorize(creds)


def ensure_columns(headers, sheet):
    """
    Ensures that all required columns (standard + legacy) exist.
    Returns updated headers list, a map of header->index, and a bool whether we added any.
    """
    col_map = {h: i for i, h in enumerate(headers)}

    required = [
        "NAME", "Email_ID", "STATUS", "SENDER", "TIMESTAMP",
        "Open_timestamp", "Open_status", "Leads_email", "Open_count",
        "Last_open_timestamp", "From", "Subject", "Campaign_name",
        "Timezone", "Start_Date", "Template",
        # standardized open flags
        "Followup1_Open", "Followup2_Open", "Followup3_Open",
        # legacy open flags
        "Opened_FW1", "Opened_FW2", "Opened_FW3"
    ]

    updated = False
    for col in required:
        if col not in col_map:
            headers.append(col)
            col_map[col] = len(headers) - 1
            sheet.update_cell(1, len(headers), col)
            updated = True

    # Return new headers list, map, and flag
    return headers, {h: i for i, h in enumerate(headers)}, updated


def update_sheet(
    sheet,
    email: str,
    sender: str,
    timestamp: str,
    sheet_name: str = None,
    subject: str = None,
    timezone: str = None,
    start_date: str = None,
    template: str = None,
    stage: str = None
):
    """
    Update existing row for email+sender match or append new.
    Marks:
      - standardized FollowupN_Open = "OPENED"
      - legacy Opened_FWN = "YES"
    """

    # 1. Read or initialize headers
    headers = sheet.row_values(1)
    if not headers:
        headers = [
            "NAME", "Email_ID", "STATUS", "SENDER", "TIMESTAMP",
            "Open_timestamp", "Open_status", "Leads_email", "Open_count",
            "Last_open_timestamp", "From", "Subject", "Campaign_name",
            "Timezone", "Start_Date", "Template"
        ]
        sheet.append_row(headers)

    headers, col_map, _ = ensure_columns(headers, sheet)

    # 2. Derive open column name from stage
    open_col = stage.replace("_Sent", "_Open") if stage else None
    if open_col and open_col not in col_map:
        headers.append(open_col)
        col_map[open_col] = len(headers) - 1
        sheet.update_cell(1, len(headers), open_col)

    # 3. Fetch body rows (skip header)
    body = sheet.get_all_values()[1:]

    # 4. Find and update matching row
    matched = False
    for ridx, row in enumerate(body, start=2):
        try:
            email_id_cell = row[col_map.get("Email_ID", -1)].strip().lower() \
                            if col_map.get("Email_ID", -1) < len(row) else ""
            sender_cell   = row[col_map.get("SENDER", -1)].strip().lower() \
                            if col_map.get("SENDER", -1) < len(row) else ""

            if email.lower() == email_id_cell and sender.lower() == sender_cell:
                # Fill missing Leads_email
                leads_idx = col_map.get("Leads_email")
                if leads_idx is not None:
                    existing = row[leads_idx] if leads_idx < len(row) else ""
                    if not existing.strip():
                        sheet.update_cell(ridx, leads_idx + 1, email)

                # Increment counts & timestamps
                try:
                    count = int(row[col_map.get("Open_count", 0)] or "0") + 1
                except ValueError:
                    count = 1

                sheet.update_cell(ridx, col_map["Open_count"] + 1, str(count))
                sheet.update_cell(ridx, col_map["Open_timestamp"] + 1, timestamp)
                sheet.update_cell(ridx, col_map["Last_open_timestamp"] + 1, timestamp)
                sheet.update_cell(ridx, col_map["Open_status"] + 1,       "OPENED")
                sheet.update_cell(ridx, col_map["From"] + 1,              sender)

                if subject:
                    sheet.update_cell(ridx, col_map["Subject"] + 1, subject)
                if sheet_name:
                    sheet.update_cell(ridx, col_map["Campaign_name"] + 1, sheet_name)
                if timezone:
                    sheet.update_cell(ridx, col_map["Timezone"] + 1, timezone)
                if start_date:
                    sheet.update_cell(ridx, col_map["Start_Date"] + 1, start_date)
                if template:
                    sheet.update_cell(ridx, col_map["Template"] + 1, template)

                # mark standardized flag
                if open_col and open_col in col_map:
                    sheet.update_cell(ridx, col_map[open_col] + 1, "OPENED")

                # mark legacy flag
                legacy_flag = None
                if open_col == "Followup1_Open":
                    legacy_flag = "Opened_FW1"
                elif open_col == "Followup2_Open":
                    legacy_flag = "Opened_FW2"
                elif open_col == "Followup3_Open":
                    legacy_flag = "Opened_FW3"

                if legacy_flag and legacy_flag in col_map:
                    sheet.update_cell(ridx, col_map[legacy_flag] + 1, "YES")

                matched = True
                break

        except Exception as e:
            app.logger.warning(f"Error updating row {ridx}: {e}")

    # 5. Append a new row if no existing match
    if not matched:
        new_row = [""] * len(headers)
        new_row[col_map["Leads_email"]]         = email
        new_row[col_map["Email_ID"]]            = email
        new_row[col_map["Open_timestamp"]]      = timestamp
        new_row[col_map["Last_open_timestamp"]] = timestamp
        new_row[col_map["Open_status"]]         = "OPENED"
        new_row[col_map["Open_count"]]          = "1"
        new_row[col_map["From"]]                = sender
        new_row[col_map["Subject"]]             = subject or ""
        new_row[col_map["Campaign_name"]]       = sheet_name or ""
        new_row[col_map["Timezone"]]            = timezone or ""
        new_row[col_map["Start_Date"]]          = start_date or ""
        new_row[col_map["Template"]]            = template or ""

        if open_col and open_col in col_map:
            new_row[col_map[open_col]] = "OPENED"

        # Also set legacy if applicable
        if open_col == "Followup1_Open":
            new_row[col_map["Opened_FW1"]] = "YES"
        elif open_col == "Followup2_Open":
            new_row[col_map["Opened_FW2"]] = "YES"
        elif open_col == "Followup3_Open":
            new_row[col_map["Opened_FW3"]] = "YES"

        sheet.append_row(new_row)
        app.logger.info("🔄 Appended new open row for email: %s", email)


@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    """
    Tracking pixel endpoint.
    Expects base64-encoded JSON metadata in the URL path.
    """
    # Decode metadata token
    try:
        token   = path.split('.')[0]
        padded  = token + "=" * (-len(token) % 4)
        payload = base64.urlsafe_b64decode(padded.encode())
        info    = json.loads(payload).get("metadata", {})
        app.logger.info(f"Decoded metadata: {info}")
    except Exception as e:
        app.logger.error("Invalid metadata: %s", e)
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    # Determine user timezone
    try:
        user_tz = pytz.timezone(info.get("timezone", "Asia/Kolkata"))
    except Exception:
        user_tz = IST

    now       = datetime.now(user_tz)
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

    # Extract metadata fields
    email       = info.get("email")
    sender      = info.get("sender")
    sheet_tab   = info.get("sheet")
    subject     = info.get("subject")
    timezone    = info.get("timezone")
    start_date  = info.get("date")
    template    = info.get("template")
    sent_time_s = info.get("sent_time")
    stage       = info.get("stage")

    # Skip tracking if the open occurs too soon (<7s)
    if sent_time_s:
        try:
            sent_dt = datetime.fromisoformat(sent_time_s)
            if sent_dt.tzinfo is None:
                sent_dt = sent_dt.replace(tzinfo=timezone.utc)  # assume UTC for naive
            now_utc = datetime.now(timezone.utc)
            if (now_utc - sent_dt).total_seconds() < 7:
                app.logger.info("Skipping early hit for %s", email)
                return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")
        except Exception as e:
            app.logger.warning(f"Early-hit check failed: {e}")

    # Open (or create) the target sheet/tab
    try:
        wb   = gc.open(MAILTRACKING_WORKBOOK)
        tabs = [ws.title for ws in wb.worksheets()]
        if not sheet_tab:
            sheet_tab = tabs[0] if tabs else "Sheet1"
        if sheet_tab not in tabs:
            wb.add_worksheet(title=sheet_tab, rows="1000", cols="20")
        sheet = wb.worksheet(sheet_tab)
    except Exception as e:
        app.logger.error("Cannot open workbook/tab: %s", e)
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    # Record the open event
    if email and sender:
        update_sheet(
            sheet,
            email=email,
            sender=sender,
            timestamp=timestamp,
            sheet_name=sheet_tab,
            subject=subject,
            timezone=timezone,
            start_date=start_date,
            template=template,
            stage=stage
        )
        app.logger.info(
            "Tracked open: %s → %s at %s (stage=%s)",
            email, sheet_tab, timestamp, stage
        )

    return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")


@app.route('/health')
def health():
    return "Tracker is live."


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
