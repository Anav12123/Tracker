from flask import Flask, request, send_file
from datetime import datetime
import base64
import json
import os
import requests

app = Flask(__name__)

#  Replace with your actual Google Apps Script Web App URL
GOOGLE_SHEET_WEBHOOK_URL = "https://script.google.com/macros/s/AKfycbza9wBiZdShn9GSzckoVnia3wa2p7hjkWOpI2PHF0x-JrGgIIqHPO2zTvh1-UlPBIY/exec"

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    # decode the base64 path → {"metadata":{"email": "..."}}
    try:
        key = path.split('.')[0]
        padded = key + '=' * (-len(key) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode())
        meta = json.loads(decoded)
        email = meta.get("metadata", {}).get("email", "Unknown")
    except Exception as e:
        email = "Error:" + str(e)

    ts = datetime.utcnow().isoformat()
    payload = {
        "type":      "open",
        "email":     email,
        "timestamp": ts
    }
    # fire the webhook
    try:
        r = requests.post(GOOGLE_SHEET_WEBHOOK_URL, json=payload)
        print("OPEN→", r.status_code, r.text)
    except Exception as e:
        print("OPEN ERR→", e)

    # local backup
    with open("opens.log","a") as f:
        f.write(f"{ts} OPENED {email}\n")

    return send_file("pixel.png", mimetype="image/png")

@app.route("/health")
def health():
    return "OK"

if __name__=="__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT",5000)))
