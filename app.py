from flask import Flask, request, send_file
from datetime import datetime
import base64
import json
import os

app = Flask(__name__)

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    try:
        # Remove file extension if present
        path = path.split('.')[0]
        padded = path + '=' * (-len(path) % 4)  # Pad base64 if necessary
        decoded = base64.urlsafe_b64decode(padded)
        metadata = json.loads(decoded)
        email = metadata.get("email", "Unknown")
    except Exception as e:
        email = f"Invalid ({e})"

    with open("opens.log", "a") as log:
        log.write(f"{datetime.now()} - OPENED: {email}\n")
    return send_file("pixel.png", mimetype="image/png")

@app.route('/health')
def health_check():
    return "Tracking server running."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
