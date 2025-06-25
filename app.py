from flask import Flask, request, send_file
from datetime import datetime
import base64
import json
import os
import requests

app = Flask(__name__)

#  Replace with your actual Google Apps Script Web App URL
GOOGLE_SHEET_WEBHOOK_URL = "https://script.google.com/macros/s/AKfycbxvCnw0fTyapsx34H0IV7KtaLaBdbivvIdiy4LMVtFXxBAiKssHUa8kOPNBgCfDBg9u/exec"

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    try:
        # Remove file extension like ".png"
        path = path.split('.')[0]

        # Base64 decode with padding fix
        padded = path + '=' * (-len(path) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode())
        metadata = json.loads(decoded)

        email = metadata.get("metadata", {}).get("email", "Unknown")
    except Exception as e:
        email = f"Error decoding: {e}"

    timestamp = str(datetime.now())

    #  Post to Google Sheets
    try:
        payload = {"timestamp": timestamp, "email": email}
        response = requests.post(GOOGLE_SHEET_WEBHOOK_URL, json=payload)
        print(f"Webhook status: {response.status_code} - {response.text}")
    except Exception as post_error:
        print(f"Failed to post to Google Sheets: {post_error}")

    #  Local backup
    with open("opens.log", "a") as log:
        log.write(f"{timestamp} - OPENED: {email}\n")

    return send_file("pixel.png", mimetype="image/png")

@app.route('/health')
def health():
    return "Tracking server is live."

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
