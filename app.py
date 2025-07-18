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

# === Google Sheets Setup ===
SCOPES             = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
DEFAULT_SHEET_NAME = "EmailTRACKV2"
IST               = pytz.timezone("Asia/Kolkata")

creds_info = json.loads(os.environ["GOOGLE_CREDS_JSON"])
creds      = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
client     = gspread.authorize(creds)

# === UA-Based Bot/Proxy Detection ===

def is_bot_ua(ua: str) -> bool:
    ua = ua.lower()
    PROXY_KEYWORDS = [
        "googleimageproxy", "apis-google", "googlewebrender",
        "feedfetcher", "curl", "wget", "python-requests",
        "headless", "phantomjs", "slurp", "crawler", "spider",
        "pingdom", "speedtest"
    ]
    return any(tok in ua for tok in PROXY_KEYWORDS)

def is_human_ua(ua: str) -> bool:
    ua = ua.lower()
    HUMAN_TOKENS = [
        "mozilla", "applewebkit", "gecko",
        "windows nt", "macintosh",
        "iphone", "android",
        "outlook", "thunderbird", "applemail"
    ]
    return any(tok in ua for tok in HUMAN_TOKENS)

def should_record():
    ua = request.headers.get("User-Agent", "")
    # 1) Drop if it looks like a proxy/headless UA
    if is_bot_ua(ua):
        return False
    # 2) Only allow if UA looks like a real browser or mail-app
    return is_human_ua(ua)

# === Sheet Updater ===

def update_sheet(sheet, email, sender, timestamp, stage=None, subject=None):
    headers = sheet.row_values(1)
    if not headers:
        # First-time header creation
        headers = [
            "Timestamp","Status","Email","Open_count","Last_Open",
            "From","Subject","Opened_FW1","Opened_FW2","Opened_FW3"
        ]
        sheet.append_row(headers)

    col_map = {h: i for i, h in enumerate(headers)}

    # Ensure columns exist
    for col in ["Status","Open_count","Last_Open","From","Subject"]:
        if col not in col_map:
            sheet.insert_cols([[col]], col=len(headers)+1)
            headers = sheet.row_values(1)
            col_map = {h: i for i, h in enumerate(headers)}

    rows = sheet.get_all_values()[1:]
    for r, row in enumerate(rows, start=2):
        if row[col_map["Email"]].strip().lower() == email.lower():
            # Update existing
            count = int(row[col_map["Open_count"]] or "0") + 1
            sheet.update_cell(r, col_map["Open_count"]+1, count)
            sheet.update_cell(r, col_map["Last_Open"]+1, timestamp)
            sheet.update_cell(r, col_map["Status"]+1, "OPENED")
            sheet.update_cell(r, col_map["From"]+1, sender)
            sheet.update_cell(r, col_map["Subject"]+1, subject or "")
            if stage:
                sc = f"Opened_{stage.upper()}"
                if sc in col_map:
                    sheet.update_cell(r, col_map[sc]+1, "YES")
            return

    # Append new
    new = [""] * len(headers)
    new[col_map["Timestamp"]]    = timestamp
    new[col_map["Status"]]       = "OPENED"
    new[col_map["Email"]]        = email
    new[col_map["Open_count"]]   = "1"
    new[col_map["Last_Open"]]    = timestamp
    new[col_map["From"]]         = sender
    new[col_map["Subject"]]      = subject or ""
    if stage:
        sc = f"Opened_{stage.upper()}"
        if sc in col_map:
            new[col_map[sc]] = "YES"
    sheet.append_row(new)

# === Transparent GIF Bytes ===

PIXEL_BYTES = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00"
    b"\xFF\xFF\xFF!\xF9\x04\x01\x00\x00\x00\x00,"
    b"\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02"
    b"L\x01\x00;"
)

# === Tracking Endpoint ===

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    # 1) Only record if UA passes human check
    if not should_record():
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    # 2) Parse metadata
    IST_NOW = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    try:
        token      = path.split('.')[0]
        padded     = token + "=" * (-len(token) % 4)
        payload    = base64.urlsafe_b64decode(padded.encode())
        meta       = json.loads(payload)
        info       = meta.get("metadata", {})
        email      = info.get("email")
        sender     = info.get("sender")
        stage      = info.get("stage")
        subject    = info.get("subject")
        sheet_name = info.get("sheet", DEFAULT_SHEET_NAME)
        sheet      = client.open(sheet_name).sheet1
    except Exception as e:
        print("⚠ Invalid metadata:", e)
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    # 3) Update sheet
    if email and sender:
        update_sheet(sheet, email, sender, IST_NOW, stage, subject)
        print(f"✅ Tracked human open: {email}")

    return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

@app.route('/health')
def health():
    return "Tracker is live and healthy."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
