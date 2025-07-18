from flask import Flask, request, send_file
from datetime import datetime
import base64, json, os, io, pytz
import ipaddress
import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)

# === Config & Google Sheets Setup ===
SCOPES            = ["https://www.googleapis.com/auth/spreadsheets",
                     "https://www.googleapis.com/auth/drive"]
DEFAULT_SHEET     = "EmailTRACKV2"
IST               = pytz.timezone("Asia/Kolkata")
GOOGLE_CREDS_JSON = os.environ["GOOGLE_CREDS_JSON"]

creds_info = json.loads(GOOGLE_CREDS_JSON)
creds      = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
client     = gspread.authorize(creds)

# === Google’s public IP blocks (simplified example) ===
GOOGLE_NETBLOCKS = [
    "64.18.0.0/20",    # Gmail image proxy  
    "66.102.0.0/20",   # googleusercontent.com
    "66.249.80.0/20",
    "72.14.192.0/18",
    "74.125.0.0/16",
    "209.85.128.0/17"
]
# Pre-parse
GOOGLE_NETWORKS = [ipaddress.ip_network(cidr) for cidr in GOOGLE_NETBLOCKS]

def ip_is_google(ip_str):
    try:
        ip = ipaddress.ip_address(ip_str)
        return any(ip in net for net in GOOGLE_NETWORKS)
    except Exception:
        return False

# === Bot & Proxy Filters ===
def is_known_bot(ua: str) -> bool:
    ua = ua.lower()
    bots = [
        "bot", "crawler", "spider", "fetch", "slurp", "preview",
        "scanner", "monitor", "pingdom", "python-requests",
        "curl", "wget", "msnbot", "bingbot", "facebookexternalhit",
        "yahoo", "ia_archiver", "phantomjs", "headless",
        "speedtest", "discordbot"
    ]
    return any(tok in ua for tok in bots)

def is_image_proxy(req) -> bool:
    ua  = req.headers.get("User-Agent", "").lower()
    via = req.headers.get("Via", "").lower()
    referer = req.headers.get("Referer", "").lower()
    xfwd = req.headers.get("X-Forwarded-For", "")

    # Common Gmail proxy tokens
    proxy_signatures = [
        "googleimageproxy", "gmailimageproxy", "googlewebrender",
        "apis-google", "googlewebpreview", "google",
        "camo.githubusercontent",  # GitHub’s image proxy
    ]
    # 1) UA hints
    if any(tok in ua for tok in proxy_signatures):
        return True

    # 2) Via header
    if "google" in via and "mail" in via:
        return True

    # 3) Referer coming from google
    if referer.startswith("https://mail.google.com"):
        return True

    # 4) Client IP belongs to known Google netblocks
    ip = req.headers.get("X-Forwarded-For", req.remote_addr).split(",")[0].strip()
    if ip_is_google(ip):
        return True

    return False

def is_bot_request(req) -> bool:
    ua = req.headers.get("User-Agent", "")
    return is_known_bot(ua) or is_image_proxy(req)

# === Sheet Updater ===
def update_sheet(sheet, email, sender, ts, stage=None, subject=None, human=False):
    headers = sheet.row_values(1)
    col_map = {h: i for i, h in enumerate(headers)}

    # Ensure required cols
    needed = [
      "Timestamp","Status","Email","Open_count","Last_Open",
      "From","Subject","Verified_Human"
    ]
    for col in needed:
        if col not in col_map:
            sheet.insert_cols([[col]], col=len(headers)+1)
            headers = sheet.row_values(1)
            col_map = {h: i for i, h in enumerate(headers)}

    data = sheet.get_all_values()[1:]
    for r, row in enumerate(data, start=2):
        if row[col_map["Email"]].strip().lower() == email.lower():
            # Update existing
            count = int(row[col_map["Open_count"]] or "0") + 1
            sheet.update_cell(r, col_map["Open_count"]+1, count)
            sheet.update_cell(r, col_map["Last_Open"]+1, ts)
            sheet.update_cell(r, col_map["Status"]+1, "OPENED")
            sheet.update_cell(r, col_map["From"]+1, sender)
            sheet.update_cell(r, col_map["Subject"]+1, subject or "")
            if human:
                sheet.update_cell(r, col_map["Verified_Human"]+1, "YES")
            if stage:
                sc = f"Opened_{stage.upper()}"
                if sc in col_map:
                    sheet.update_cell(r, col_map[sc]+1, "YES")
            return

    # Append new row
    row = [""] * len(headers)
    row[col_map["Timestamp"]]      = ts
    row[col_map["Status"]]         = "OPENED"
    row[col_map["Email"]]          = email
    row[col_map["Open_count"]]     = "1"
    row[col_map["Last_Open"]]      = ts
    row[col_map["From"]]           = sender
    row[col_map["Subject"]]        = subject or ""
    row[col_map["Verified_Human"]] = "YES" if human else ""
    if stage and f"Opened_{stage.upper()}" in col_map:
        row[col_map[f"Opened_{stage.upper()}"]] = "YES"
    sheet.append_row(row)

# === Transparent GIF ===
def blank_pixel():
    return send_file(io.BytesIO(
        b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00"
        b"\xFF\xFF\xFF!\xF9\x04\x01\x00\x00\x00\x00,"
        b"\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02"
        b"L\x01\x00;"
    ), mimetype="image/gif")

# === Pixel Endpoint ===
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    # 1) Drop every bot/proxy immediately
    if is_bot_request(request):
        return blank_pixel()

    # 2) Parse metadata
    IST_NOW = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    try:
        token = path.split('.')[0]
        padded = token + "=" * (-len(token) % 4)
        meta = json.loads(base64.urlsafe_b64decode(padded))
        info = meta.get("metadata", {})
        email, sender = info.get("email"), info.get("sender")
        stage, subject = info.get("stage"), info.get("subject")
        sheet_name = info.get("sheet", DEFAULT_SHEET_NAME)
        sheet = client.open(sheet_name).sheet1

        # Ensure header row
        if not sheet.get_all_values():
            sheet.append_row([
                "Timestamp","Status","Email","Open_count","Last_Open","From","Subject",
                "Followup1_Sent","Opened_FW1","Followup2_Sent","Opened_FW2",
                "Followup3_Sent","Opened_FW3","Verified_Human"
            ])
    except Exception as e:
        print("⚠ Invalid metadata:", e)
        return blank_pixel()

    # 3) Record initial open (unverified human)
    if email and sender:
        try:
            update_sheet(sheet, email, sender, IST_NOW, stage, subject, human=False)
        except Exception as err:
            print("❌ Sheet update failed:", err)

    return blank_pixel()

# === Human Verified Link ===
@app.route('/human/<token>')
def human_open(token):
    IST_NOW = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    try:
        padded = token + "=" * (-len(token) % 4)
        meta   = json.loads(base64.urlsafe_b64decode(padded))
        info   = meta.get("metadata", {})
        email, sender = info.get("email"), info.get("sender")
        stage, subject = info.get("stage"), info.get("subject")
        sheet_name = info.get("sheet", DEFAULT_SHEET_NAME)
        sheet = client.open(sheet_name).sheet1

        update_sheet(sheet, email, sender, IST_NOW, stage, subject, human=True)
    except Exception as e:
        print("❌ Human open failed:", e)

    return blank_pixel()

# === Health Check ===
@app.route('/health')
def health():
    return "Tracker is live and healthy."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
