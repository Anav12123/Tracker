from flask import Flask, send_file
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
    col_map = {h: i for i, h in enumerate(headers)}
    # Required base columns (idempotent)
    required = [
        "NAME","Email_ID","STATUS","SENDER","TIMESTAMP",
        "Open_timestamp","Open_status","Leads_email","Open_count",
        "Last_open_timestamp","From","Subject","Campaign_name",
        "Timezone","Start_Date","Template",
        # stage-open flags
        "Followup1_Open","Followup2_Open","Followup3_Open"
    ]
    updated = False
    for col in required:
        if col not in col_map:
            headers.append(col)
            col_map[col] = len(headers) - 1
            sheet.update_cell(1, len(headers), col)
            updated = True
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
    Also marks the stage-specific Open column (e.g., Followup1_Open) = "OPENED".
    """
    # 1. Ensure header
    headers = sheet.row_values(1)
    if not headers:
        headers = [
            "NAME","Email_ID","STATUS","SENDER","TIMESTAMP",
            "Open_timestamp","Open_status","Leads_email","Open_count",
            "Last_open_timestamp","From","Subject","Campaign_name",
            "Timezone","Start_Date","Template"
        ]
        sheet.append_row(headers)

    headers, col_map, _ = ensure_columns(headers, sheet)

    # derive open column name from stage
    open_col = stage.replace("_Sent","_Open") if stage else None
    if open_col and open_col not in col_map:
        headers.append(open_col)
        col_map[open_col] = len(headers) - 1
        sheet.update_cell(1, len(headers), open_col)

    # 2. Get sheet data (skip headers)
    body = sheet.get_all_values()[1:]

    # 3. Find matching row
    matched = False
    for ridx, row in enumerate(body, start=2):
        try:
            lead_email = row[col_map.get("Leads_email", -1)].strip().lower() if len(row) > col_map.get("Leads_email", -1) else ""
            email_id   = row[col_map.get("Email_ID", -1)].strip().lower() if len(row) > col_map.get("Email_ID", -1) else ""
            sender_cell = row[col_map.get("SENDER", -1)].strip().lower() if len(row) > col_map.get("SENDER", -1) else ""

            if email.lower() == email_id and sender.lower() == sender_cell:
                # Fill missing Leads_email
                if "Leads_email" in col_map:
                    leads_email_cell = row[col_map["Leads_email"]] if col_map["Leads_email"] < len(row) else ""
                    if not leads_email_cell.strip():
                        sheet.update_cell(ridx, col_map["Leads_email"] + 1, email)

                # Increment open count
                try:
                    count = int(row[col_map.get("Open_count", 0)] or "0") + 1
                except Exception:
                    count = 1

                sheet.update_cell(ridx, col_map["Open_count"] + 1, str(count))
                sheet.update_cell(ridx, col_map["Open_timestamp"] + 1, timestamp)
                sheet.update_cell(ridx, col_map["Last_open_timestamp"] + 1, timestamp)
                sheet.update_cell(ridx, col_map["Open_status"] + 1, "OPENED")
                sheet.update_cell(ridx, col_map["From"] + 1, sender)

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

                # mark the stage-specific Open flag
                if open_col and open_col in col_map:
                    sheet.update_cell(ridx, col_map[open_col] + 1, "OPENED")

                matched = True
                break
        except Exception as e:
            app.logger.warning(f"Error matching row {ridx}: {e}")

    # 4. Append if no match
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

    # TZ
    try:
        user_tz = pytz.timezone(info.get("timezone", "Asia/Kolkata"))
    except Exception as e:
        app.logger.warning("Invalid timezone '%s'. Using IST. Error: %s", info.get("timezone"), e)
        user_tz = IST

    now = datetime.now(user_tz)
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

    # Extract fields
    email       = info.get("email")
    sender      = info.get("sender")
    sheet_tab   = info.get("sheet")
    sheet_name  = info.get("sheet_name")
    subject     = info.get("subject")
    timezone    = info.get("timezone")
    start_date  = info.get("date")
    template    = info.get("template")
    sent_time_s = info.get("sent_time")
    stage       = info.get("stage")  # <-- important

    # Skip early hits < 7s
    if sent_time_s:
        try:
            sent_dt = datetime.fromisoformat(sent_time_s)
            if (now - sent_dt).total_seconds() < 7:
                app.logger.info("Skipping early hit for %s", email)
                return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")
        except Exception:
            pass

    # Open workbook & tab
    try:
        wb   = gc.open(MAILTRACKING_WORKBOOK)
        tabs = [ws.title for ws in wb.worksheets()]
        if not sheet_tab:
            sheet_tab = tabs[0] if tabs else "USA"
        if sheet_tab not in tabs:
            wb.add_worksheet(title=sheet_tab, rows="1000", cols="20")
        sheet = wb.worksheet(sheet_tab)
    except Exception as e:
        app.logger.error("Cannot open workbook/tab: %s", e)
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    # Record the open
    if email and sender:
        update_sheet(
            sheet,
            email=email,
            sender=sender,
            timestamp=timestamp,
            sheet_name=sheet_name,
            subject=subject,
            timezone=timezone,
            start_date=start_date,
            template=template,
            stage=stage           # pass stage through
        )
        app.logger.info("Tracked open: %s → %s at %s (stage=%s)", email, sheet_tab, timestamp, stage)

    return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

@app.route('/health')
def health():
    return "Tracker is live."

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
