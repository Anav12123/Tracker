from flask import Flask, request, send_file
from datetime import datetime
import base64
import json
import os
import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)

# === Google Sheets Setup ===
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

SPREADSHEET_NAME = "EmailTRACKV2"

# Load credentials from Render environment variable
creds_info = json.loads(os.environ["GOOGLE_CREDS_JSON"])
creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
client = gspread.authorize(creds)
sheet = client.open(SPREADSHEET_NAME).sheet1

def update_sheet(email, sender, timestamp, stage=None):
    headers = sheet.row_values(1)
    col_map = {key.strip(): idx for idx, key in enumerate(headers)}
    data = sheet.get_all_values()[1:]  # Exclude header row
    found = False

    for i, row in enumerate(data):
        if row[col_map["Email"]] == email:
            row_num = i + 2
            current_count = int(row[col_map["Open_count"]] or "0") + 1

            sheet.update_cell(row_num, col_map["Open_count"] + 1, current_count)
            sheet.update_cell(row_num, col_map["Last_Open"] + 1, timestamp)
            sheet.update_cell(row_num, col_map["Status"] + 1, "OPENED")
            if "From" in col_map:
                sheet.update_cell(row_num, col_map["From"] + 1, sender)

            #  Mark open with YES instead of timestamp
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
        if "From" in col_map:
            new_row[col_map["From"]] = sender
        if stage:
            open_col = {
                "fw_1": "Opened_FW1",
                "fw_2": "Opened_FW2",
                "fw_3": "Opened_FW3"
            }.get(stage)
            if open_col and open_col in col_map:
                new_row[col_map[open_col]] = "YES"
        sheet.append_row(new_row)

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    email = sender = stage = None
    timestamp = str(datetime.now())

    try:
        token = path.split('.')[0]
        padded = token + '=' * (-len(token) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode())
        metadata = json.loads(decoded)
        email = metadata.get("metadata", {}).get("email")
        sender = metadata.get("metadata", {}).get("sender")
        stage  = metadata.get("metadata", {}).get("stage")
    except Exception as e:
        print(f"⚠ Invalid metadata: {e}")

    if email and sender:
        try:
            update_sheet(email, sender, timestamp, stage=stage)
            print(f" Tracked: {email} from {sender} (stage: {stage})")
        except Exception as err:
            print(f" Sheet update failed: {err}")

        with open("opens.log", "a") as log:
            log.write(f"{timestamp} - OPENED: {email} (from {sender}, stage: {stage})\n")

    return send_file("pixel.png", mimetype="image/png")

@app.route('/health')
def health():
    return "Tracker is live and healthy."

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
