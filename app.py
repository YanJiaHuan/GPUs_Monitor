import os
from datetime import datetime
from flask import Flask, jsonify, render_template

from monitor import Monitor

CONFIG_PATH = os.environ.get("GPU_MON_CONFIG", "config.yaml")

app = Flask(__name__)
monitor = Monitor(CONFIG_PATH)


@app.get("/")
def index():
    return render_template("index.html", refresh=monitor.refresh_seconds)


@app.get("/api/status")
def api_status():
    data = monitor.get_status()
    data["updated_at_iso"] = datetime.utcfromtimestamp(data["updated_at"]).isoformat() + "Z"
    return jsonify(data)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), debug=True)
