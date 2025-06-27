from flask import Flask, request, send_file
from datetime import datetime
import base64
import json
import os
import requests

app = Flask(__name__)

# Your existing Apps Script Web App URL
GOOGLE_SHEET_WEBHOOK_URL = "https://script.google.com/macros/s/AKfycbz6Wc0gBTSA2A0dfOHowgIHKyqgRoLrkN_ufbrizsshmLBl7FJ0E9UvsKesQkGYZGVH/exec"

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    email = None
    sender = None
    try:
        # exactly as before: strip extension, pad & decode
        token   = path.split('.')[0]
        padded  = token + '=' * (-len(token) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode())
        metadata = json.loads(decoded)

        # safe‐get your email and the original sender
        email  = metadata.get("metadata", {}).get("email")
        sender = metadata.get("metadata", {}).get("sender")
    except Exception:
        # any decode/parsing error → email/sender stay None
        pass

    timestamp = str(datetime.now())

    # only post & log when we got both email and sender
    if email and sender:
        try:
            payload = {
                "timestamp": timestamp,
                "email":     email,
                "sender":    sender
            }
            response = requests.post(GOOGLE_SHEET_WEBHOOK_URL, json=payload)
            print(f"Webhook status: {response.status_code} - {response.text}")
        except Exception as post_error:
            print(f"Failed to post to Google Sheets: {post_error}")

        with open("opens.log", "a") as log:
            log.write(f"{timestamp} - OPENED: {email} (from {sender})\n")

    # always return the 1×1 tracking pixel
    return send_file("pixel.png", mimetype="image/png")

@app.route('/health')
def health():
    return "Tracking server is live."

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
