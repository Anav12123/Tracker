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

# === Config & Google Sheets Setup ===
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
DEFAULT_SHEET_NAME = "EmailTRACKV2"
IST = pytz.timezone("Asia/Kolkata")

creds_info = json.loads(os.environ["GOOGLE_CREDS_JSON"])
creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
client = gspread.authorize(creds)

# === Bot & Proxy Filters ===

def is_known_bot(ua: str) -> bool:
    ua = ua.lower()
    BOT_SUBSTRINGS = [
        "bot", "crawler", "spider", "fetch", "slurp",
        "preview", "scanner", "monitor", "pingdom",
        "python-requests", "curl", "wget",
        "msnbot", "bingbot", "facebookexternalhit",
        "yahoo", "ia_archiver", "phantomjs",
        "headless", "speedtest"
    ]
    return any(tok in ua for tok in BOT_SUBSTRINGS)

def is_image_proxy(req) -> bool:
    ua = req.headers.get("User-Agent", "").lower()
    via = req.headers.get("Via", "").lower()
    return any([
        "googleimageproxy" in ua,
        "googleusercontent" in ua,
        "apis-google" in ua,
        "gmail" in ua,
        "google" in via and "mail" in via,
        "google" in ua and "mail" in ua,
    ])

def is_bot_request(req) -> bool:
    ua = req.headers.get("User-Agent", "")
    return is_known_bot(ua) or is_image_proxy(req)

# === Spreadsheet Updater ===

def update_sheet(sheet, email, sender, timestamp, stage=None, subject=None, human=False):
    headers = sheet.row_values(1)
    col_map = {h: i for i, h in enumerate(headers)}

    # Ensure all required columns exist
    for col in ["Timestamp", "Status", "Email", "Open_count", "Last_Open", "From", "Subject", "Verified_Human"]:
        if col not in col_map:
            sheet.insert_cols([[col]], col=len(headers)+1)
            headers = sheet.row_values(1)
            col_map = {h: i for i, h in enumerate(headers)}

    rows = sheet.get_all_values()[1:]
    for idx, row in enumerate(rows, start=2):
        if row[col_map["Email"]].strip().lower() == email.strip().lower():
            count = int(row[col_map["Open_count"]] or "0") + 1
            sheet.update_cell(idx, col_map["Open_count"]+1, count)
            sheet.update_cell(idx, col_map["Last_Open"]+1, timestamp)
            sheet.update_cell(idx, col_map["Status"]+1, "OPENED")
            sheet.update_cell(idx, col_map["From"]+1, sender)
            sheet.update_cell(idx, col_map["Subject"]+1, subject or "")
            if human:
                sheet.update_cell(idx, col_map["Verified_Human"]+1, "YES")
            if stage:
                stage_col = f"Opened_{stage.upper()}"
                if stage_col in col_map:
                    sheet.update_cell(idx, col_map[stage_col]+1, "YES")
            return

    new_row = [""] * len(headers)
    new_row[col_map["Timestamp"]]     = timestamp
    new_row[col_map["Status"]]        = "OPENED"
    new_row[col_map["Email"]]         = email
    new_row[col_map["Open_count"]]    = "1"
    new_row[col_map["Last_Open"]]     = timestamp
    new_row[col_map["From"]]          = sender
    new_row[col_map["Subject"]]       = subject or ""
    new_row[col_map["Verified_Human"]]= "YES" if human else ""
    if stage:
        stage_col = f"Opened_{stage.upper()}"
        if stage_col in col_map:
            new_row[col_map[stage_col]] = "YES"
    sheet.append_row(new_row)

# === 1x1 Pixel (transparent GIF)
def blank_pixel():
    return send_file(io.BytesIO(
        b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00"
        b"\xFF\xFF\xFF!\xF9\x04\x01\x00\x00\x00\x00,"
        b"\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02"
        b"L\x01\x00;"
    ), mimetype="image/gif")

# === Primary Tracking Pixel (could be Gmail proxy)
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    # === FILTER bots & proxies immediately
    if is_bot_request(request):
        print("🚫 Blocked proxy/bot UA:", request.headers.get("User-Agent", ""))
        return blank_pixel()

    IST_NOW = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    try:
        token = path.split('.')[0]
        padded = token + '=' * (-len(token) % 4)
        meta = json.loads(base64.urlsafe_b64decode(padded.encode()))
        info = meta.get("metadata", {})
        email = info.get("email")
        sender = info.get("sender")
        stage = info.get("stage")
        subject = info.get("subject")
        sheet_name = info.get("sheet", DEFAULT_SHEET_NAME)
        sheet = client.open(sheet_name).sheet1
        if not sheet.get_all_values():
            sheet.append_row([
                "Timestamp", "Status", "Email", "Open_count", "Last_Open", "From", "Subject",
                "Followup1_Sent", "Opened_FW1", "Followup2_Sent", "Opened_FW2", "Followup3_Sent", "Opened_FW3",
                "Verified_Human"
            ])
    except Exception as e:
        print("⚠ Invalid metadata:", e)
        return blank_pixel()

    if email and sender:
        try:
            update_sheet(sheet, email, sender, IST_NOW, stage, subject, human=False)
            print(f" Tracked proxy/initial open: {email} from {sender}")
        except Exception as err:
            print(" Sheet update failed:", err)

    return blank_pixel()

# === Human Verified Endpoint ===
@app.route('/human/<token>')
def human_open(token):
    IST_NOW = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    try:
        padded = token + '=' * (-len(token) % 4)
        meta = json.loads(base64.urlsafe_b64decode(padded.encode()))
        info = meta.get("metadata", {})
        email = info.get("email")
        sender = info.get("sender")
        stage = info.get("stage")
        subject = info.get("subject")
        sheet_name = info.get("sheet", DEFAULT_SHEET_NAME)
        sheet = client.open(sheet_name).sheet1
        update_sheet(sheet, email, sender, IST_NOW, stage, subject, human=True)
        print(f" Verified human open: {email}")
    except Exception as e:
        print(" Failed to process human open:", e)

    return blank_pixel()

# === Health Check ===
@app.route('/health')
def health():
    return "Tracker is live and healthy."

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
