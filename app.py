from flask import Flask, request, send_file
from datetime import datetime
import pytz
import base64
import json
import io
import os
import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)

# === Constants ===
IST = pytz.timezone("Asia/Kolkata")
DEFAULT_SHEET_NAME = "EmailTRACKV3"
PIXEL_BYTES = base64.b64decode("R0lGODlhAQABAPAAAP///wAAACH5BAAAAAAALAAAAAABAAEAAAICRAEAOw==")

# === Google Sheets Setup ===
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = Credentials.from_service_account_file("creds.json", scopes=SCOPES)
client = gspread.authorize(creds)

# === Sheet Update Function ===
def update_sheet(sheet, email, sender, timestamp, stage, subject):
    try:
        cell = sheet.find(email)
        row = cell.row
        sheet.update_cell(row, 4, timestamp)  # Column D = Open Timestamp
        sheet.update_cell(row, 5, stage)      # Column E = Stage
        sheet.update_cell(row, 6, subject)    # Column F = Subject
    except gspread.exceptions.CellNotFound:
        sheet.append_row([email, sender, timestamp, stage, subject])

# === Tracking Pixel Route ===
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

        # Prevent false open if within 10 seconds of sent_time
        if sent_time_str:
            sent_time = datetime.strptime(sent_time_str, "%Y-%m-%d %H:%M:%S%z")
            delta = (now - sent_time.astimezone(IST)).total_seconds()
            if delta < 10:
                print(f" Ignored early proxy hit for {email} (Δ = {delta:.2f}s)")
                return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

        sheet = client.open(sheet_name).sheet1

    except Exception as e:
        print(" Invalid metadata or decoding error:", e)
        return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

    if email and sender:
        update_sheet(sheet, email, sender, timestamp, stage, subject)
        print(f"Tracked open for {email} at {timestamp}")

    return send_file(io.BytesIO(PIXEL_BYTES), mimetype="image/gif")

if __name__ == "__main__":
    app.run(debug=True)
