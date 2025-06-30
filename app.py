#!/usr/bin/env python3
import os, io, json, base64
from datetime import datetime
from flask import Flask, send_file, abort
import gspread
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1

# === CONFIG ===
SERVICE_ACCOUNT_FILE = "service-account.json"
GOOGLE_SHEET_NAME    = "EmailTRACKV2"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# 1×1 transparent PNG or GIF
PIXEL_PATH = "pixel.png"  # include this in your project

# === SHEET AUTH ===
creds  = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
sheet  = gspread.authorize(creds).open(GOOGLE_SHEET_NAME).sheet1
HEADERS= [h.strip() for h in sheet.row_values(1)]

# Stage→column mapping
STAGE_COL = {
    "fw_1": "Opened_FW1",
    "fw_2": "Opened_FW2",
    "fw_3": "Opened_FW3"
}

app = Flask(__name__)

@app.route('/<token>')
def track(token):
    # 1) Decode the metadata
    try:
        padded   = token + '=' * (-len(token) % 4)
        raw      = base64.urlsafe_b64decode(padded)
        payload  = json.loads(raw)
        md       = payload["metadata"]
        email    = md["email"].strip().lower()
        sender   = md["sender"]
        stage    = md.get("stage")
    except Exception:
        return abort(400)

    # 2) Find the row for this email
    try:
        cell = sheet.find(email, in_column=HEADERS.index("Email")+1)
    except Exception:
        # no such email → still return pixel
        return send_file(PIXEL_PATH, mimetype="image/png")

    row = cell.row
    now = datetime.utcnow().isoformat()

    # 3) Increment Open_count
    oc_cell   = sheet.cell(row, HEADERS.index("Open_count")+1)
    new_count = int(oc_cell.value or "0") + 1
    sheet.update_cell(row, oc_cell.col, new_count)

    # 4) Last_Open + Status + From
    sheet.update_cell(row, HEADERS.index("Last_Open")+1, now)
    sheet.update_cell(row, HEADERS.index("Status"   )+1, "OPENED")
    sheet.update_cell(row, HEADERS.index("From"     )+1, sender)

    # 5) Mark the correct Opened_FW* column
    col_name = STAGE_COL.get(stage)
    if col_name and col_name in HEADERS:
        sheet.update_cell(row, HEADERS.index(col_name)+1, "YES")

    # 6) Return the pixel image
    return send_file(PIXEL_PATH, mimetype="image/png")

@app.route('/health')
def health():
    return "OK"

if __name__ == "__main__":
    port = int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0", port=port)
