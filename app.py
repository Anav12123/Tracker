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

# === Bot & Gmail-Proxy Detection ===
def is_bot(ua: str) -> bool:
    ua = ua.lower()
    # your existing filters plus curl/wget etc
    BOT_KEYWORDS = [
        "bot", "crawler", "scanner", "proofpoint", "mimecast",
        "curl", "wget", "python-requests", "outlook", "microsoft"
    ]
    return any(tok in ua for tok in BOT_KEYWORDS)

def is_gmail_proxy(req) -> bool:
    """Detect Gmail’s automatic image proxy via UA, Via or Referer."""
    ua     = req.headers.get("User-Agent", "").lower()
    via    = req.headers.get("Via", "").lower()
    refer  = req.headers.get("Referer", "").lower()

    # Gmail/Image proxy signatures
    if "googleimageproxy" in ua:               # new Gmail UA
        return True
    if "apis-google" in ua:                    # another Gmail UA
        return True
    if "googlewebrender" in ua:                # Gmail mobile preview
        return True
    if refer.startswith("https://mail.google.com"):
        return True
    if "google" in via and "mail" in via:
        return True

    return False

def is_bot_request(req) -> bool:
    ua = req.headers.get("User-Agent", "")
    return is_bot(ua) or is_gmail_proxy(req)


# === Sheet Updater ===
def update_sheet(sheet, email, sender, timestamp, stage=None, subject=None):
    headers = sheet.row_values(1)
    col_map = {h: i for i, h in enumerate(headers)}

    # ensure essential columns exist
    for col in ["Timestamp","Status","Email","Open_count","Last_Open","From","Subject"]:
        if col not in col_map:
            sheet.insert_cols([[col]], col=len(headers)+1)
            headers = sheet.row_values(1)
            col_map = {h: i for i, h in enumerate(headers)}

    rows = sheet.get_all_values()[1:]
    for r, row in enumerate(rows, start=2):
        if row[col_map["Email"]].strip().lower() == email.lower():
            # update existing
            count = int(row[col_map["Open_count"]] or "0") + 1
            sheet.update_cell(r, col_map["Open_count"]+1, count)
            sheet.update_cell(r, col_map["Last_Open"]+1, timestamp)
            sheet.update_cell(r, col_map["Status"]+1, "OPENED")
            sheet.update_cell(r, col_map["From"]+1, sender)
            sheet.update_cell(r, col_map["Subject"]+1, subject or "")
            if stage:
                stage_col = f"Opened_{stage.upper()}"
                if stage_col in col_map:
                    sheet.update_cell(r, col_map[stage_col]+1, "YES")
            return

    # append new row
    new_row = [""] * len(headers)
    new_row[col_map["Timestamp"]]   = timestamp
    new_row[col_map["Status"]]      = "OPENED"
    new_row[col_map["Email"]]       = email
    new_row[col_map["Open_count"]]  = "1"
    new_row[col_map["Last_Open"]]   = timestamp
    new_row[col_map["From"]]        = sender
    new_row[col_map["Subject"]]     = subject or ""
    if stage:
        stage_col = f"Opened_{stage.upper()}"
        if stage_col in col_map:
            new_row[col_map[stage_col]] = "YES"
    sheet.append_row(new_row)

# === Transparent GIF BYTES ===
GIF_BYTES = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00"
    b"\xFF\xFF\xFF!\xF9\x04\x01\x00\x00\x00\x00,"
    b"\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02"
    b"L\x01\x00;"
)

# === Tracking Endpoint ===
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    # 1) Immediately drop any bot / proxy hit
    if is_bot_request(request):
        return send_file(io.BytesIO(GIF_BYTES), mimetype="image/gif")

    # 2) Parse metadata and ensure sheet exists
    IST_NOW = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    try:
        token     = path.split('.')[0]
        padded    = token + '=' * (-len(token) % 4)
        metadata  = json.loads(base64.urlsafe_b64decode(padded.encode()))
        info      = metadata.get("metadata", {})
        email     = info.get("email")
        sender    = info.get("sender")
        stage     = info.get("stage")
        subject   = info.get("subject")
        sheet_name= info.get("sheet", DEFAULT_SHEET_NAME)
        sheet     = client.open(sheet_name).sheet1

        # add header row if missing
        if not sheet.get_all_values():
            sheet.append_row([
                "Timestamp","Status","Email","Open_count","Last_Open",
                "From","Subject","Followup1_Sent","Opened_FW1",
                "Followup2_Sent","Opened_FW2","Followup3_Sent","Opened_FW3"
            ])
    except Exception as e:
        print("⚠ Invalid metadata:", e)
        return send_file(io.BytesIO(GIF_BYTES), mimetype="image/gif")

    # 3) Record only real human opens
    if email and sender:
        try:
            update_sheet(sheet, email, sender, IST_NOW, stage, subject)
            print(f"✅ Tracked human open: {email}")
        except Exception as err:
            print("❌ Sheet update failed:", err)

    # 4) Return the transparent GIF
    return send_file(io.BytesIO(GIF_BYTES), mimetype="image/gif")


@app.route('/health')
def health():
    return "Tracker is live and healthy."

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
