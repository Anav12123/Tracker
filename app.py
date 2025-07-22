
from flask import Flask, request, send_file
from datetime import datetime
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

# Timezone for timestamping
IST = pytz.timezone("Asia/Kolkata")

# Name of your MailTracking Google Sheets workbook
MAILTRACKING_WORKBOOK = "MailTracking"

# Transparent 1×1 GIF (bytes)
PIXEL_BYTES = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00"
    b"\xFF\xFF\xFF!\xF9\x04\x01\x00\x00\x00\x00,"
    b"\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02"
    b"L\x01\x00;"
)

# === Google Sheets client setup ===
creds_info = json.loads(os.environ["GOOGLE_CREDS_JSON"])
creds      = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
gc         = gspread.authorize(creds)


def update_sheet(sheet, email, sender, timestamp,
                 stage=None, subject=None):
    """
    Update an existing row for `email` or append a new one.
    The sheet's first row is treated as headers.
    """
    # Ensure header row exists
    headers = sheet.row_values(1)
    if not headers:
        headers = [
            "Timestamp", "Status", "Email", "Open_count", "Last_Open",
            "From", "Subject", "Opened_USA", "Opened_ISRAEL",
            "Opened_APAC", "Opened_M.E"
        ]
        sheet.append_row(headers)

    # Map header→column index
    col_map = {h: i for i, h in enumerate(headers)}

    # Add missing core columns if any
    for col in ["Status", "Open_count", "Last_Open", "From", "Subject"]:
        if col not in col_map:
            sheet.insert_cols([[col]], col=len(headers) + 1)
            headers = sheet.row_values(1)
            col_map = {h: i for i, h in enumerate(headers)}

    # Search for existing email row
    data = sheet.get_all_values()[1:]
    for row_idx, row in enumerate(data, start=2):
        if row[col_map["Email"]].strip().lower() == email.lower():
            # Update counters and timestamp
            count = int(row[col_map["Open_count"]] or "0") + 1
            sheet.update_cell(row_idx, col_map["Open_count"] + 1, count)
            sheet.update_cell(row_idx, col_map["Last_Open"]    + 1, timestamp)
            sheet.update_cell(row_idx, col_map["Status"]       + 1, "OPENED")
            sheet.update_cell(row_idx, col_map["From"]         + 1, sender)
            sheet.update_cell(row_idx, col_map["Subject"]      + 1, subject or "")
            if stage:
                sc = f"Opened_{stage}"
                if sc in col_map:
                    sheet.update_cell(row_idx, col_map[sc] + 1, "YES")
            return

    # Append new row if email not found
    new_row = [""] * len(headers)
    new_row[col_map["Timestamp"]]   = timestamp
    new_row[col_map["Status"]]      = "OPENED"
    new_row[col_map["Email"]]       = email
    new_row[col_map["Open_count"]]  = "1"
    new_row[col_map["Last_Open"]]   = timestamp
    new_row[col_map["From"]]        = sender
    new_row[col_map["Subject"]]     = subject or ""
    if stage:
        sc = f"Opened_{stage}"
        if sc in col_map:
            new_row[col_map[sc]] = "YES"

    sheet.append_row(new_row)


@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    """
    Pixel tracking endpoint. Metadata is passed via the URL
    path as base64(JSON), e.g. /<token>.gif
    """
    now       = datetime.now(IST)
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

    # Decode metadata
    try:
        token   = path.split('.')[0]
        padded  = token + "=" * (-len(token) % 4)
        payload = base64.urlsafe_b64decode(padded.encode())
        meta    = json.loads(payload)
        info    = meta.get("metadata", {})

        email   = info.get("email")
        sender  = info.get("sender")
        stage   = info.get("stage")        # "USA", "Israel", "APAC", "M.E", etc.
        subject = info.get("subject")
        sheet_tab = info.get("sheet", None)  # name of the worksheet/tab

    except Exception as e:
        app.logger.error("Invalid metadata payload: %s", e)
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    # Open MailTracking workbook
    try:
        wb   = gc.open(MAILTRACKING_WORKBOOK)
        tabs = [ws.title for ws in wb.worksheets()]

        # If metadata didn’t supply a tab, default to “USA”
        if not sheet_tab:
            sheet_tab = "USA"

        # Create worksheet if missing
        if sheet_tab not in tabs:
            app.logger.info("Creating missing tab '%s' in '%s'", sheet_tab,
                            MAILTRACKING_WORKBOOK)
            wb.add_worksheet(title=sheet_tab, rows="1000", cols="20")

        sheet = wb.worksheet(sheet_tab)
        app.logger.info("Tracking to %s → %s", MAILTRACKING_WORKBOOK, sheet_tab)

    except Exception as e:
        app.logger.error("Could not open workbook/tab: %s", e)
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    # Record the open event
    if email and sender:
        update_sheet(sheet, email, sender, timestamp,
                     stage=stage, subject=subject)
        app.logger.info("Recorded open for %s at %s", email, timestamp)

    return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")


@app.route('/health')
def health():
    return "Tracker is live."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
