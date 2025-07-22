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
IST = pytz.timezone("Asia/Kolkata")
MAILTRACKING_WORKBOOK = "MailTracking"

# 1×1 transparent GIF
PIXEL_BYTES = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00"
    b"\xFF\xFF\xFF!\xF9\x04\x01\x00\x00\x00\x00,"
    b"\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02"
    b"L\x01\x00;"
)

# === GSpread client ===
creds_info = json.loads(os.environ["GOOGLE_CREDS_JSON"])
creds      = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
gc         = gspread.authorize(creds)

def update_sheet(
    sheet: gspread.Worksheet,
    email: str,
    sender: str,
    timestamp: str,
    stage: str = None,
    subject: str = None,
    timezone: str = None,
    start_date: str = None,
    template: str = None
):
    """
    Update an existing row for `email`, or append a new one.
    Ensures header row contains all required columns, then writes.
    """
    # 1) Determine current headers
    headers = sheet.row_values(1)
    if not headers:
        headers = [
            "Timestamp","Status","Email","Open_count","Last_Open",
            "From","Subject","Timezone","Start_Date","Template",
            "Opened_USA","Opened_ISRAEL","Opened_APAC","Opened_M.E"
        ]
        sheet.append_row(headers)

    # 2) Map header -> zero-based index
    col_map = {h: i for i, h in enumerate(headers)}

    # 3) Ensure all required columns exist in header row
    required = ["Status","Open_count","Last_Open","From","Subject",
                "Timezone","Start_Date","Template"]
    stage_cols = [f"Opened_{s}" for s in ["USA","ISRAEL","APAC","M.E"]]
    for col in required + stage_cols:
        if col not in col_map:
            headers.append(col)
            col_map[col] = len(headers) - 1
            # write the new header cell
            sheet.update_cell(1, len(headers), col)

    # 4) Fetch all existing rows (below header)
    body = sheet.get_all_values()[1:]  # list of lists
    # 5) Try to find an existing row for this email
    for ridx, row in enumerate(body, start=2):
        if row[col_map["Email"]].strip().lower() == email.lower():
            # Update counters and metadata
            count = int(row[col_map["Open_count"]] or "0") + 1
            sheet.update_cell(ridx, col_map["Open_count"]+1, str(count))
            sheet.update_cell(ridx, col_map["Last_Open"]+1, timestamp)
            sheet.update_cell(ridx, col_map["Status"]+1, "OPENED")
            sheet.update_cell(ridx, col_map["From"]+1, sender)
            if subject:
                sheet.update_cell(ridx, col_map["Subject"]+1, subject)
            if timezone:
                sheet.update_cell(ridx, col_map["Timezone"]+1, timezone)
            if start_date:
                sheet.update_cell(ridx, col_map["Start_Date"]+1, start_date)
            if template:
                sheet.update_cell(ridx, col_map["Template"]+1, template)
            if stage:
                sc = f"Opened_{stage}"
                sheet.update_cell(ridx, col_map[sc]+1, "YES")
            return

    # 6) Email not found → append new row
    new_row = [""] * len(headers)
    new_row[col_map["Timestamp"]]   = timestamp
    new_row[col_map["Status"]]      = "OPENED"
    new_row[col_map["Email"]]       = email
    new_row[col_map["Open_count"]]  = "1"
    new_row[col_map["Last_Open"]]   = timestamp
    new_row[col_map["From"]]        = sender
    if subject:
        new_row[col_map["Subject"]] = subject
    if timezone:
        new_row[col_map["Timezone"]] = timezone
    if start_date:
        new_row[col_map["Start_Date"]] = start_date
    if template:
        new_row[col_map["Template"]] = template
    if stage:
        sc = f"Opened_{stage}"
        new_row[col_map[sc]] = "YES"

    sheet.append_row(new_row)


@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    """
    Tracking pixel endpoint.  
    Expects metadata JSON base64‐encoded in the URL path.
    """
    now       = datetime.now(IST)
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

    # 1) Decode metadata payload
    try:
        token   = path.split('.')[0]
        padded  = token + "=" * (-len(token) % 4)
        payload = base64.urlsafe_b64decode(padded.encode())
        info    = json.loads(payload)["metadata"]
    except Exception as e:
        app.logger.error("Invalid metadata payload: %s", e)
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    email      = info.get("email")
    sender     = info.get("sender")
    stage      = info.get("stage")          # e.g. "USA", "APAC", etc.
    subject    = info.get("subject")
    timezone   = info.get("timezone")
    start_date = info.get("start_date")     # "YYYY-MM-DD"
    template   = info.get("template")
    sheet_tab  = info.get("sheet", "USA")   # fallback to "USA"

    # 2) Open MailTracking workbook & tab
    try:
        wb   = gc.open(MAILTRACKING_WORKBOOK)
        tabs = [ws.title for ws in wb.worksheets()]
        if sheet_tab not in tabs:
            app.logger.info("Creating tab '%s' in '%s'", sheet_tab, MAILTRACKING_WORKBOOK)
            wb.add_worksheet(title=sheet_tab, rows="1000", cols="20")
        sheet = wb.worksheet(sheet_tab)
    except Exception as e:
        app.logger.error("Cannot open workbook/tab: %s", e)
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    # 3) Record the open event
    if email and sender:
        update_sheet(
            sheet,
            email=email,
            sender=sender,
            timestamp=timestamp,
            stage=stage,
            subject=subject,
            timezone=timezone,
            start_date=start_date,
            template=template
        )
        app.logger.info("Tracked open: email=%s tab=%s at %s", email, sheet_tab, timestamp)

    # 4) Return the transparent GIF
    return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")


@app.route('/health')
def health():
    return "Tracker is live."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
