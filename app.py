from flask import Flask, request, send_file
from datetime import datetime
import base64
import json

app = Flask(__name__)

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def track(path):
    try:
        # Clean path if it has a file extension like .png
        path = path.split('.')[0]

        # Add padding and decode base64 safely
        padded = path + '=' * (-len(path) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode())
        metadata = json.loads(decoded)
        email = metadata.get('email', 'Unknown')
    except Exception as e:
        email = f"Error decoding: {e}"

    with open("opens.log", "a") as log:
        log.write(f"{datetime.now()} - OPENED: {email}\n")

    return send_file("pixel.png", mimetype="image/png")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
