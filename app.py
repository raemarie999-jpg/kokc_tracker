import os, json, threading
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, render_template_string
import requests

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024

API_KEY = os.environ.get("WETHR_API_KEY", "")
DATA_DIR = "/data"

def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)

def safe_float(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except:
        return None

STATIONS = ["KOKC", "KPHL"]

def make_state():
    return {
        "obs": None,
        "forecasts": {},
        "accuracy": {},
        "log": [],
        "consensus_snapshots": []
    }

states = {s: make_state() for s in STATIONS}

def get_state(station):
    return states.get(station, states["KOKC"])

def add_log(msg, level="info", station="KOKC"):
    st = get_state(station)
    st["log"].insert(0, {"t": datetime.utcnow().strftime("%H:%M:%S"), "msg": msg, "level": level})
    st["log"] = st["log"][:50]

def okc_now():
    return datetime.utcnow() - timedelta(hours=5)

# -------------------------
# CONSENSUS ENGINE FIXED
# -------------------------
def compute_consensus(st):
    acc = st.get("accuracy", {}) or {}
    forecasts = st.get("forecasts", {}) or {}

    models = set(acc.keys()) | set(forecasts.keys())
    models.discard("NWS")

    w_sum = 0
    w_total = 0

    for m in models:
        fcst = forecasts.get(m, {})
        a = acc.get(m, {})

        raw = safe_float(fcst.get("high"))
        corr = safe_float(a.get("correction"))

        if raw is None:
            continue

        adj = raw + (corr or 0)

        mae = safe_float(a.get("mae")) or 1.0
        w = 1.0 / max(mae, 0.1)

        w_sum += adj * w
        w_total += w

    return round(w_sum / w_total, 1) if w_total else None


# -------------------------
# API
# -------------------------
@app.route("/api/state")
def api_state():
    station = request.args.get("station", "KOKC").upper()
    st = get_state(station)

    rows = []
    acc = st.get("accuracy", {})
    forecasts = st.get("forecasts", {})

    for m in set(acc.keys()) | set(forecasts.keys()):
        fcst = forecasts.get(m, {})
        a = acc.get(m, {})

        raw = safe_float(fcst.get("high"))
        corr = safe_float(a.get("correction"))

        rows.append({
            "model": m,
            "adj": (raw + (corr or 0)) if raw is not None else None,
            "mae": safe_float(a.get("mae")),
            "weight": 1.0 / max(safe_float(a.get("mae")) or 1.0, 0.1)
        })

    return jsonify({
        "station": station,
        "consensus": compute_consensus(st),
        "rows": rows,
        "log": st["log"]
    })


@app.route("/api/accuracy", methods=["POST"])
def save_accuracy():
    station = request.args.get("station", "KOKC").upper()
    st = get_state(station)
    st["accuracy"] = request.json or {}
    add_log("accuracy updated", "ok", station)
    return jsonify({"ok": True})


# -------------------------
# FRONTEND (FIXED - REAL HTML)
# -------------------------
HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>KOKC Tracker</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { font-family: Arial; background:#0b0f14; color:#fff; margin:0; padding:20px; }
        .card { background:#161b22; padding:15px; border-radius:10px; margin-bottom:10px; }
        h1 { margin-top:0; }
        .row { display:flex; justify-content:space-between; }
        .good { color:#3fb950; }
        .bad { color:#f85149; }
    </style>
</head>
<body>

<h1>KOKC Weather Model Tracker</h1>
<div class="card">
    <div>Consensus: <span id="consensus">...</span></div>
</div>

<div class="card" id="models"></div>

<script>
async function load() {
    const res = await fetch('/api/state');
    const data = await res.json();

    document.getElementById("consensus").innerText = data.consensus;

    let html = "";
    for (let r of data.rows) {
        html += `<div class='row'>
            <div>${r.model}</div>
            <div>${r.adj ?? '-'}</div>
            <div>${r.mae ?? '-'}</div>
        </div>`;
    }

    document.getElementById("models").innerHTML = html;
}

load();
setInterval(load, 10000);
</script>

</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(HTML)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
