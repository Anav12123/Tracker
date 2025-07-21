from flask import Flask, request, send_file
from datetime import datetime
from ipaddress import ip_address, ip_network
import base64
import json
import os
import io
import pytz
import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)

# === Config ===
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
DEFAULT_SHEET_NAME = "EmailTRACKV2"
SUSPICIOUS_SHEET_NAME = "SuspiciousIPs"
IST = pytz.timezone("Asia/Kolkata")

GMAIL_CIDRS = [
    "66.102.0.0/20",
    "64.233.160.0/19",
    "74.125.0.0/16",
    "108.177.0.0/17",
    "66.249.80.0/20"
]

# === Google Sheets Auth ===
creds_info = json.loads(os.environ["GOOGLE_CREDS_JSON"])
creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
client = gspread.authorize(creds)

# === Utility: Check if IP is from Gmail proxy ===
def is_proxy_ip(ip_str):
    try:
        ip_obj = ip_address(ip_str)
        return any(ip_obj in ip_network(cidr) for cidr in GMAIL_CIDRS)
    except:
        return False

# === Log suspicious IP ===
def log_suspicious_open(ip, email, user_agent, delta, timestamp):
    try:
        sheet = client.open(DEFAULT_SHEET_NAME)
        try:
            ws = sheet.worksheet(SUSPICIOUS_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            ws = sheet.add_worksheet(title=SUSPICIOUS_SHEET_NAME, rows=1000, cols=10)
            ws.append_row(["Timestamp", "IP", "Email", "UserAgent", "Delta"])

        ws.append_row([timestamp, ip, email, user_agent, f"{delta:.2f}s"])
    except Exception as e:
        print("⚠ Failed to log suspicious open:", e)

# === Sheet Updater ===
def update_sheet(sheet, email, sender, timestamp, stage=None, subject=None):
    headers = sheet.row_values(1)
    if not headers:
        headers = [
            "Timestamp", "Status", "Email", "Open_count", "Last_Open",
            "From", "Subject", "Opened_FW1", "Opened_FW2", "Opened_FW3"
        ]
        sheet.append_row(headers)

    col_map = {h: i for i, h in enumerate(headers)}
    for col in ["Status", "Open_count", "Last_Open", "From", "Subject"]:
        if col not in col_map:
            sheet.insert_cols([[col]], col=len(headers)+1)
            headers = sheet.row_values(1)
            col_map = {h: i for i, h in enumerate(headers)}

    rows = sheet.get_all_values()[1:]
    for r, row in enumerate(rows, start=2):
        if row[col_map["Email"]].strip().lower() == email.lower():
            count = int(row[col_map["Open_count"]] or "0") + 1
            sheet.update_cell(r, col_map["Open_count"] + 1, count)
            sheet.update_cell(r, col_map["Last_Open"] + 1, timestamp)
            sheet.update_cell(r, col_map["Status"] + 1, "OPENED")
            sheet.update_cell(r, col_map["From"] + 1, sender)
            sheet.update_cell(r, col_map["Subject"] + 1, subject or "")
            if stage:
                sc = f"Opened_{stage.upper()}"
                if sc in col_map:
                    sheet.update_cell(r, col_map[sc] + 1, "YES")
            return

    new = [""] * len(headers)
    new[col_map["Timestamp"]] = timestamp
    new[col_map["Status"]] = "OPENED"
    new[col_map["Email"]] = email
    new[col_map["Open_count"]] = "1"
    new[col_map["Last_Open"]] = timestamp
    new[col_map["From"]] = sender
    new[col_map["Subject"]] = subject or ""
    if stage:
        sc = f"Opened_{stage.upper()}"
        if sc in col_map:
            new[col_map[sc]] = "YES"
    sheet.append_row(new)

# === Transparent Pixel ===
PIXEL_BYTES = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00"
    b"\xFF\xFF\xFF!\xF9\x04\x01\x00\x00\x00\x00,"
    b"\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02"
    b"L\x01\x00;"
)

# === Track Endpoint ===
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    now = datetime.now(IST)
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

    try:
        token = path.split('.')[0]
        padded = token + "=" * (-len(token) % 4)
        payload = base64.urlsafe_b64decode(padded.encode())
        meta = json.loads(payload)
        info = meta.get("metadata", {})

        email = info.get("email")
        sender = info.get("sender")
        stage = info.get("stage")
        subject = info.get("subject")
        sheet_name = info.get("sheet", DEFAULT_SHEET_NAME)
        sent_time_str = info.get("sent_time")
        print(sent_time_str)
        sent_time = datetime.strptime(sent_time_str, "%Y-%m-%d %H:%M:%S%z") if sent_time_str else None
        print(sent_time)
        ip = request.remote_addr
        user_agent = request.headers.get("User-Agent", "")

        suspicious = False
        if sent_time:
            delta = (now - sent_time.astimezone(IST)).total_seconds()
            if delta < 10 and is_proxy_ip(ip):
                suspicious = True
                log_suspicious_open(ip, email, user_agent, delta, timestamp)

        if suspicious:
            print(f"⚠ Ignored early proxy open from {ip}")
            return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

        sheet = client.open(sheet_name).sheet1

    except Exception as e:
        print("⚠ Invalid metadata or decoding error:", e)
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    if email and sender:
        update_sheet(sheet, email, sender, timestamp, stage, subject)
        print(f" Tracked open for {email} at {timestamp}")

    return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

@app.route('/health')
def health():
    return "Tracker is live and working."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
