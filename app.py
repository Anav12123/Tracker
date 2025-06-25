from flask import Flask, request, send_file
from datetime import datetime
import base64
import json
import os
import requests

app = Flask(__name__)

# Replace with your actual Google Apps Script Webhook URL
GOOGLE_SHEET_WEBHOOK_URL = "https://script.google.com/macros/s/AKfycby8zsDYdgyNB8LJCnzy7-jNAgSws-8Mp9AKlKjN0zD1MfiG-i90EdEGE7XCo4l_htfh/exec"

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    try:
        # Remove file extension like ".png" if present
        path = path.split('.')[0]
        
        # Pad and decode the base64 path
        padded = path + '=' * (-len(path) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode())
        metadata = json.loads(decoded)
        email = metadata.get("email", "Unknown")
    except Exception as e:
        email = f"Error decoding: {e}"

    timestamp = str(datetime.now())

    # Log to Google Sheet via webhook
    try:
        payload = {"timestamp": timestamp, "email": email}
        requests.post(GOOGLE_SHEET_WEBHOOK_URL, json=payload)
    except Exception as post_error:
        print(f"Failed to post to Google Sheets: {post_error}")

    # Backup local log
    with open("opens.log", "a") as log:
        log.write(f"{timestamp} - OPENED: {email}\n")

    # Serve tracking pixel
    return send_file("pixel.png", mimetype="image/png")

@app.route('/health')
def health():
    return "Tracking server is live."

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
