
from flask import Flask, send_file, request
from datetime import datetime, timezone as dt_timezone
import base64
import json
import os
import io
import pytz
import gspread
from gspread.utils import rowcol_to_a1
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


def ensure_columns(sheet, required):
    """
    Guarantees all required columns exist in row 1.
    Expands sheet width if needed and writes headers in one batch.
    Returns final_headers and a header->index map.
    """
    current = sheet.row_values(1) or []
    header_set = set(current)
    final_headers = list(current)

    for col in required:
        if col not in header_set:
            final_headers.append(col)
            header_set.add(col)

    # Expand columns if needed
    needed_cols = len(final_headers)
    if sheet.col_count < needed_cols:
        # Prefer add_cols, fallback to resize
        try:
            sheet.add_cols(needed_cols - sheet.col_count)
        except Exception:
            sheet.resize(rows=sheet.row_count, cols=needed_cols)

    # Batch update the header row
    end_a1 = rowcol_to_a1(1, needed_cols)
    sheet.update(f"A1:{end_a1}", [final_headers])

    col_map = {h: i for i, h in enumerate(final_headers)}
    return final_headers, col_map


def update_sheet(
    sheet,
    email: str,
    sender: str,
    timestamp: str,
    sheet_name: str = None,
    subject: str = None,
    timezone_str: str = None,
    start_date: str = None,
    template: str = None,
    stage: str = None
):
    open_col = stage.replace("_Sent", "_Open") if stage else None

    required = [
        "NAME", "Email_ID", "STATUS", "SENDER", "TIMESTAMP",
        "Open_timestamp", "Open_status", "Leads_email", "Open_count",
        "Last_open_timestamp", "From", "Subject", "Campaign_name",
        "Timezone", "Start_Date", "Template",
        "Followup1_Open", "Followup2_Open", "Followup3_Open"
    ]
    if open_col and open_col not in required:
        required.append(open_col)

    headers, col_map = ensure_columns(sheet, required)

    def cell_value(row, header_name):
        idx = col_map.get(header_name, -1)
        return row[idx] if 0 <= idx < len(row) else ""

    body = sheet.get_all_values()[1:]
    email_l = (email or "").strip().lower()
    sender_l = (sender or "").strip().lower()

    matched = False
    for ridx, row in enumerate(body, start=2):
        try:
            email_cell = (cell_value(row, "Email_ID") or cell_value(row, "Leads_email")).strip().lower()
            sender_cell = (cell_value(row, "SENDER") or cell_value(row, "From")).strip().lower()

            if email_l == email_cell and sender_l == sender_cell:
                count = int(cell_value(row, "Open_count") or "0") + 1 if cell_value(row, "Open_count") else 1

                updates = [
                    ("Open_count", str(count)),
                    ("Open_timestamp", timestamp),
                    ("Last_open_timestamp", timestamp),
                    ("Open_status", "OPENED"),
                    ("From", sender),
                    ("SENDER", sender),
                    ("Leads_email", email),
                    ("Subject", subject or ""),
                    ("Campaign_name", sheet_name or ""),
                    ("Timezone", timezone_str or ""),
                    ("Start_Date", start_date or ""),
                    ("Template", template or "")
                ]

                if open_col and open_col in col_map:
                    updates.append((open_col, "OPENED"))

                for key, val in updates:
                    sheet.update_cell(ridx, col_map[key] + 1, val)

                matched = True
                break
        except Exception as e:
            app.logger.warning(f"Error updating row {ridx}: {e}")

    if matched:
        return

    new_row = [""] * len(headers)
    for key, val in {
        "Email_ID": email,
        "Leads_email": email,
        "Open_timestamp": timestamp,
        "Last_open_timestamp": timestamp,
        "Open_status": "OPENED",
        "Open_count": "1",
        "From": sender,
        "SENDER": sender,
        "Subject": subject or "",
        "Campaign_name": sheet_name or "",
        "Timezone": timezone_str or "",
        "Start_Date": start_date or "",
        "Template": template or "",
        open_col: "OPENED" if open_col and open_col in col_map else ""
    }.items():
        if key in col_map:
            new_row[col_map[key]] = val

    sheet.append_row(new_row)
    app.logger.info("Appended new open row for email: %s", email)



@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    """
    Tracking pixel endpoint.
    Expects base64 urlsafe encoded JSON in the URL path.
    Accepts either {"metadata": {...}} or a flat dict as the payload.
    """
    # Decode metadata token from path or fallback to query param m
    info = {}
    try:
        token = request.args.get("m") or (path or "").split('.')[0]
        padded = token + "=" * (-len(token) % 4)
        payload = base64.urlsafe_b64decode(padded.encode()).decode("utf-8", errors="ignore")
        raw = json.loads(payload)
        info = raw.get("metadata", raw) or {}
        app.logger.info(f"Decoded metadata: {info}")
    except Exception as e:
        app.logger.error(f"Invalid metadata: {e}")
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    # Determine user timezone safely
    tz_str = info.get("timezone", "Asia/Kolkata")
    try:
        user_tz = pytz.timezone(tz_str)
    except Exception:
        user_tz = IST

    now_local = datetime.now(user_tz)
    timestamp = now_local.strftime("%Y-%m-%d %H:%M:%S")

    # Extract metadata fields with safe names
    email       = info.get("email")
    sender      = info.get("sender")
    sheet_tab   = info.get("sheet")
    subject     = info.get("subject")
    timezone_str = info.get("timezone")
    # Accept both keys for start date
    start_date  = info.get("start_date") or info.get("date")
    template    = info.get("template")
    sent_time_s = info.get("sent_time")
    stage       = info.get("stage")

    # Early hit guard less than seven seconds after sent time
    if sent_time_s:
        try:
            # Support trailing Z by normalizing to offset form
            s = sent_time_s.replace("Z", "+00:00")
            sent_dt = datetime.fromisoformat(s)
            if sent_dt.tzinfo is None:
                sent_dt = sent_dt.replace(tzinfo=dt_timezone.utc)
            now_utc = datetime.now(dt_timezone.utc)
            if (now_utc - sent_dt).total_seconds() < 7:
                app.logger.info("Skipping early hit for %s", email)
                return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")
        except Exception as e:
            app.logger.warning(f"Early-hit check failed: {e}")

    # Open or create the workbook and target tab
    try:
        wb = gc.open(MAILTRACKING_WORKBOOK)
        tabs = [ws.title for ws in wb.worksheets()]
        if not sheet_tab:
            sheet_tab = tabs[0] if tabs else "Sheet1"
        if sheet_tab not in tabs:
            wb.add_worksheet(title=sheet_tab, rows="1000", cols="50")
        sheet = wb.worksheet(sheet_tab)
    except Exception as e:
        app.logger.error(f"Cannot open workbook or tab: {e}")
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    # Record the open event if we have essentials
    if email and sender:
        update_sheet(
            sheet,
            email=email,
            sender=sender,
            timestamp=timestamp,
            sheet_name=sheet_tab,
            subject=subject,
            timezone_str=timezone_str,
            start_date=start_date,
            template=template,
            stage=stage
        )
    else:
        app.logger.warning("Missing essentials email or sender")

    # Always return the pixel
    return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")


@app.route('/health')
def health():
    return "Tracker is live."


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
