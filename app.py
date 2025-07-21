from flask import Flask, request, send_file
from datetime import datetime
import base64
import json
import os
import io
import pytz
import gspread
from google.oauth2.service_account import Credentials
from ipaddress import ip_address, ip_network
import time

app = Flask(__name__)

# === Google Sheets Setup ===
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
DEFAULT_SHEET_NAME = "EmailTRACKV2"
IST = pytz.timezone("Asia/Kolkata")

creds_info = json.loads(os.environ["GOOGLE_CREDS_JSON"])
creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
client = gspread.authorize(creds)
GMAIL_CIDRS = []


def is_proxy_ip(ip):
    try:
        ip_obj = ip_address(ip)
        return any(ip_obj in ip_network(cidr) for cidr in GMAIL_CIDRS)
    except:
        return False

# === Suspicious IP Tracking ===
SUSPICIOUS_IP_CACHE = {
    "ips": set(),
    "last_fetched": 0
}

def get_suspicious_ips():
    now = time.time()
    if now - SUSPICIOUS_IP_CACHE["last_fetched"] > 300:
        try:
            sheet = client.open(DEFAULT_SHEET_NAME).worksheet("SuspiciousIPs")
            values = sheet.col_values(1)[1:]
            SUSPICIOUS_IP_CACHE["ips"] = set(values)
            SUSPICIOUS_IP_CACHE["last_fetched"] = now
        except Exception as e:
            print("⚠️ Error fetching Suspicious IPs:", e)
    return SUSPICIOUS_IP_CACHE["ips"]

def log_suspicious_ip(ip, delta, email="", sender=""):
    try:
        sheet = client.open(DEFAULT_SHEET_NAME)
        try:
            ws = sheet.worksheet("SuspiciousIPs")
        except:
            ws = sheet.add_worksheet(title="SuspiciousIPs", rows="1000", cols="4")
            ws.append_row(["IP", "Detected_At", "Delta_sec", "Sample_Email"])

        now = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        ws.append_row([ip, now, f"{delta:.2f}", email])
        print(f"🚨 Logged suspicious IP: {ip}")
    except Exception as e:
        print("⚠️ Failed to log suspicious IP:", e)

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

        ip = request.remote_addr
        user_agent = request.headers.get("User-Agent", "")

        sent_time = datetime.strptime(sent_time_str, "%Y-%m-%d %H:%M:%S%z") if sent_time_str else None

        suspicious_ips = get_suspicious_ips()
        if ip in suspicious_ips:
            print(f"🚫 Skipping known suspicious IP: {ip}")
            return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

        if sent_time and (is_proxy_ip(ip) or "GoogleImageProxy" in user_agent or ip.startswith("66.249.")):
            delta = (datetime.now(pytz.utc) - sent_time).total_seconds()
            if delta < 5:
                log_suspicious_ip(ip, delta, email=email, sender=sender)
                print(f"⚠️ Early proxy open from {ip} — ignored (Δ = {delta:.2f}s)")
                return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

        sheet = client.open(sheet_name).sheet1

    except Exception as e:
        print("⚠ Invalid metadata or decoding error:", e)
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    if email and sender:
        update_sheet(sheet, email, sender, timestamp, stage, subject)
        print(f"✅ Tracked open for {email} at {timestamp}")

    return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

@app.route('/health')
def health():
    return "Tracker is live and working."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
