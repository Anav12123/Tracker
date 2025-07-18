from flask import Flask, request, send_file
from datetime import datetime
import base64, json, os, io, pytz, ipaddress
import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)

# === Config & Google Sheets Setup ===
SCOPES             = ["https://www.googleapis.com/auth/spreadsheets",
                      "https://www.googleapis.com/auth/drive"]
DEFAULT_SHEET_NAME = "EmailTRACKV2"
IST                = pytz.timezone("Asia/Kolkata")

creds_info = json.loads(os.environ["GOOGLE_CREDS_JSON"])
creds      = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
client     = gspread.authorize(creds)

# GIF bytes for 1×1 transparent pixel
PIXEL = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00"
    b"\xFF\xFF\xFF!\xF9\x04\x01\x00\x00\x00\x00,"
    b"\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02"
    b"L\x01\x00;"
)

# Google image-proxy IP ranges (common prefixes)
GOOGLE_NETS = [
    "64.18.0.0/20", "66.102.0.0/20", "66.249.80.0/20",
    "72.14.192.0/18", "74.125.0.0/16", "209.85.128.0/17"
]
GOOGLE_NETWORKS = [ipaddress.ip_network(c) for c in GOOGLE_NETS]

def client_ip():
    xff = request.headers.get("X-Forwarded-For", request.remote_addr)
    return xff.split(",")[0].strip()

def ip_is_google(ip):
    try:
        ip_obj = ipaddress.ip_address(ip)
        return any(ip_obj in net for net in GOOGLE_NETWORKS)
    except:
        return False

# Only accept “real” mail‐client or browser UAs
def is_human_ua(ua: str) -> bool:
    ua = ua.lower()
    # Must look like a browser or desktop/mobile mail client:
    return any(tok in ua for tok in [
        "mozilla", "applewebkit", "gecko", "windows nt", "macintosh",
        "iphone", "android", "outlook", "thunderbird", "applemail"
    ])

def is_bot_ua(ua: str) -> bool:
    ua = ua.lower()
    for tok in [
        "googleimageproxy", "apis-google", "feedfetcher", "curl", "wget",
        "python-requests", "headless", "phantomjs", "slurp", "crawler",
        "spider", "preview", "scanner", "pingdom", "speedtest"
    ]:
        if tok in ua:
            return True
    return False

def should_record():
    ua = request.headers.get("User-Agent", "")
    ip = client_ip()
    # 1) Block known bots/proxies by UA
    if is_bot_ua(ua):
        return False
    # 2) Block Gmail’s proxy by IP or headers
    if ip_is_google(ip):
        return False
    # 3) Only allow real browsers/mail-clients
    if not is_human_ua(ua):
        return False
    return True

def update_sheet(sheet, email, sender, ts, stage=None, subject=None):
    # Ensure header row
    headers = sheet.row_values(1)
    if not headers:
        headers = [
            "Timestamp","Status","Email","Open_count","Last_Open",
            "From","Subject","Opened_FW1","Opened_FW2","Opened_FW3"
        ]
        sheet.append_row(headers)
    col_map = {h:i for i,h in enumerate(headers)}

    # Create missing cols
    for col in ["Status","Open_count","Last_Open","From","Subject"]:
        if col not in col_map:
            sheet.insert_cols([[col]], col=len(headers)+1)
            headers = sheet.row_values(1)
            col_map = {h:i for i,h in enumerate(headers)}

    rows = sheet.get_all_values()[1:]
    for r, row in enumerate(rows, start=2):
        if row[col_map["Email"]].strip().lower() == email.lower():
            count = int(row[col_map["Open_count"]] or "0") + 1
            sheet.update_cell(r, col_map["Open_count"]+1, count)
            sheet.update_cell(r, col_map["Last_Open"]+1, ts)
            sheet.update_cell(r, col_map["Status"]+1, "OPENED")
            sheet.update_cell(r, col_map["From"]+1, sender)
            sheet.update_cell(r, col_map["Subject"]+1, subject or "")
            if stage:
                sc = f"Opened_{stage.upper()}"
                if sc in col_map:
                    sheet.update_cell(r, col_map[sc]+1, "YES")
            return

    # Append new if not found
    new = [""] * len(headers)
    new[col_map["Timestamp"]]    = ts
    new[col_map["Status"]]       = "OPENED"
    new[col_map["Email"]]        = email
    new[col_map["Open_count"]]   = "1"
    new[col_map["Last_Open"]]    = ts
    new[col_map["From"]]         = sender
    new[col_map["Subject"]]      = subject or ""
    if stage:
        sc = f"Opened_{stage.upper()}"
        if sc in col_map:
            new[col_map[sc]] = "YES"
    sheet.append_row(new)

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    # Only record if this looks like a real human open
    if not should_record():
        return send_file(io.BytesIO(PIXEL), mimetype="image/gif")

    # Parse metadata
    IST_NOW = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    try:
        token     = path.split(".")[0]
        padded    = token + "=" * (-len(token) % 4)
        meta      = json.loads(base64.urlsafe_b64decode(padded.encode()))
        info      = meta.get("metadata", {})
        email     = info.get("email")
        sender    = info.get("sender")
        stage     = info.get("stage")
        subject   = info.get("subject")
        sheet_name= info.get("sheet", DEFAULT_SHEET_NAME)
        sheet     = client.open(sheet_name).sheet1
    except Exception as e:
        print("Invalid metadata:", e)
        return send_file(io.BytesIO(PIXEL), mimetype="image/gif")

    if email and sender:
        update_sheet(sheet, email, sender, IST_NOW, stage, subject)
        print(f"✅ Human open recorded: {email}")

    return send_file(io.BytesIO(PIXEL), mimetype="image/gif")

@app.route('/health')
def health():
    return "Tracker is live."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
