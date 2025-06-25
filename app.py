from flask import Flask, request, send_file
from datetime import datetime
import base64
import json
import os
import requests

app = Flask(__name__)

# Replace with your actual Google Apps Script Web App URL
GOOGLE_SHEET_WEBHOOK_URL = "https://script.google.com/macros/s/AKfycbyGXuBDG8BooiYFv8q0V39lAKj0GAZAKhE1LQ5o7c2DgZloLiSBRDyaY_LfcIo9bLIn/exec"

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    try:
        path = path.split('.')[0]  # Remove any file extension
        padded = path + '=' * (-len(path) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode())
        metadata = json.loads(decoded)

        # Access email nested under 'metadata'
        email = metadata.get("metadata", {}).get("email", "Unknown")
    except Exception as e:
        email = f"Error decoding: {e}"

    timestamp = str(datetime.now())

    try:
        payload = {"timestamp": timestamp, "email": email}
        response = requests.post(GOOGLE_SHEET_WEBHOOK_URL, json=payload)
        print(f"Webhook status: {response.status_code} - {response.text}")
    except Exception as post_error:
        print(f"Failed to post to Google Sheets: {post_error}")

    with open("opens.log", "a") as log:
        log.write(f"{timestamp} - OPENED: {email}\n")

    return send_file("pixel.png", mimetype="image/png")

@app.route('/health')
def health():
    return "Tracking server is live."

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
