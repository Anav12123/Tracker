from flask import Flask, request, send_file
from datetime import datetime
import base64
import json
import os
import gspread
from google.oauth2.service_account import Credentials
import pytz
import io

app = Flask(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
DEFAULT_SHEET_NAME = "EmailTRACKV2"

creds_info = json.loads(os.environ["GOOGLE_CREDS_JSON"])
creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
client = gspread.authorize(creds)
IST = pytz.timezone("Asia/Kolkata")

def is_bot(user_agent):
    KNOWN_BOTS = ["google", "proxy", "crawler", "scanner", "preview", "fetch", "urlcheck",
                  "defense", "proofpoint", "barracuda", "mimecast", "outlook", "microsoft"]
    return any(bot in user_agent.lower() for bot in KNOWN_BOTS)

def update_sheet(sheet, email, sender, timestamp, stage=None, subject=None):
    headers = sheet.row_values(1)
    col_map = {key.strip(): idx for idx, key in enumerate(headers)}

    # Auto-create essential columns if missing
    for col in ["Subject", "Email", "Open_count", "Last_Open", "Status", "Timestamp"]:
        if col not in col_map:
            sheet.insert_cols([[col]], col=len(headers) + 1)
            headers = sheet.row_values(1)
            col_map = {key.strip(): idx for idx, key in enumerate(headers)}

    data = sheet.get_all_values()[1:]
    found = False

    for i, row in enumerate(data):
        if row[col_map["Email"]].strip().lower() == email.strip().lower():
            row_num = i + 2
            if row[col_map["Status"]].strip().upper() == "OPENED":
                print(f"🔁 Already marked OPENED for {email}")
                return
            current_count = int(row[col_map["Open_count"]] or "0") + 1
            sheet.update_cell(row_num, col_map["Open_count"] + 1, current_count)
            sheet.update_cell(row_num, col_map["Last_Open"] + 1, timestamp)
            sheet.update_cell(row_num, col_map["Status"] + 1, "OPENED")
            sheet.update_cell(row_num, col_map["From"] + 1, sender)
            sheet.update_cell(row_num, col_map["Subject"] + 1, subject or "")
            if stage:
                open_col = {
                    "fw_1": "Opened_FW1",
                    "fw_2": "Opened_FW2",
                    "fw_3": "Opened_FW3"
                }.get(stage)
                if open_col and open_col in col_map:
                    sheet.update_cell(row_num, col_map[open_col] + 1, "YES")
            found = True
            break

    if not found:
        new_row = ["" for _ in headers]
        new_row[col_map["Timestamp"]] = timestamp
        new_row[col_map["Status"]] = "OPENED"
        new_row[col_map["Email"]] = email
        new_row[col_map["Open_count"]] = 1
        new_row[col_map["Last_Open"]] = timestamp
        new_row[col_map["From"]] = sender
        new_row[col_map["Subject"]] = subject or ""
        if stage:
            open_col = {
                "fw_1": "Opened_FW1",
                "fw_2": "Opened_FW2",
                "fw_3": "Opened_FW3"
            }.get(stage)
            if open_col and open_col in col_map:
                new_row[col_map[open_col]] = "YES"
        sheet.append_row(new_row)
        print(f"➕ Added new OPENED row for {email}")

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    email = sender = stage = subject = None
    timestamp = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    user_agent = request.headers.get("User-Agent", "").lower()
    ip = request.headers.get("X-Forwarded-For", request.remote_addr)
    confidence = "High" if not is_bot(user_agent) else "Low"

    try:
        token = path.split('.')[0]
        padded = token + '=' * (-len(token) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode())
        meta = json.loads(decoded).get("metadata", {})
        email = meta.get("email")
        sender = meta.get("sender")
        stage = meta.get("stage")
        subject = meta.get("subject")
        sheet_name = meta.get("sheet", DEFAULT_SHEET_NAME)
        sheet = client.open(sheet_name).sheet1
    except Exception as e:
        print(f"⚠ Invalid metadata: {e}")
        return send_file(io.BytesIO(b''), mimetype="image/gif")

    if email and sender and confidence == "High":
        try:
            update_sheet(sheet, email, sender, timestamp, stage, subject)
            print(f"✅ Tracked: {email} from {sender} (stage: {stage})")
        except Exception as err:
            print(f"❌ Sheet update failed: {err}")
    else:
        print(f"⚠️ Bot or missing metadata: UA={user_agent}")

    with open("opens.log", "a") as log:
        log.write(f"{timestamp} - {confidence.upper()} OPEN: {email} "
                  f"(IP: {ip}, UA: {user_agent}, sender: {sender}, subject: {subject}, stage: {stage})\n")

    gif = b'GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xFF\xFF\xFF!' \
          b'\xF9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01' \
          b'\x00\x00\x02\x02L\x01\x00;'
    return send_file(io.BytesIO(gif), mimetype="image/gif")

@app.route("/sendgrid/events", methods=["POST"])
def sendgrid_events():
    try:
        events = request.get_json(force=True)
        sheet = client.open(DEFAULT_SHEET_NAME).sheet1
        for event in events:
            if event.get("event") == "open":
                email = event.get("email")
                ua = event.get("useragent", "")
                if is_bot(ua):
                    print(f"⚠ Bot detected: {email}")
                    continue
                timestamp = datetime.fromtimestamp(event.get("timestamp")).astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")
                update_sheet(sheet, email, "SendGrid", timestamp)
        return "OK", 200
    except Exception as e:
        print(f"❌ Webhook error: {e}")
        return "Error", 500

@app.route("/health")
def health():
    return "Tracker is live and healthy."

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
