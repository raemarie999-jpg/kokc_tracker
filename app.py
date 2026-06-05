import os, json, time, threading
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, render_template_string
import requests

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024

API_KEY = os.environ.get("WETHR_API_KEY", "")
DATA_DIR = "/data"
PACING_FILE = f"{DATA_DIR}/pacing_snapshots.json"
HISTORY_FILE = f"{DATA_DIR}/daily_history.json"

def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)

def load_json_file(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except:
        return default

def save_json_file(path, data):
    try:
        ensure_data_dir()
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, path)
        return True
    except Exception as e:
        add_log(f"Save error {path}: {e}", "err")
        return False

STATIONS = ["KOKC", "KPHL"]

ALL_KNOWN_MODELS = [
    "ARPEGE","HRRR","UKMO","LAV-MOS","NAM","RAP","GEM-GDPS","NAM-MOS","NBM",
    "NAM4KM","GFS","ICON","GFS-MOS","ECMWF-HRES","GEFS","JMA","RDPS","SREF"
]

def make_state():
    return {
        "obs": None,
        "wethr_high": None,
        "forecasts": {},
        "accuracy": {},
        "last_updated": None,
        "errors": [],
        "log": [],
        "today_avg_pace": [],
        "consensus_snapshots": [],
    }

states = {s: make_state() for s in STATIONS}

def get_state(station="KOKC"):
    return states.get(station, states["KOKC"])

def add_log(msg, level="info", station="KOKC"):
    st = get_state(station)
    entry = {"t": datetime.now().strftime("%H:%M:%S"), "msg": msg, "level": level}
    st["log"].insert(0, entry)
    st["log"] = st["log"][:120]
    print(f"[{station}] {msg}")

# ---------------- SAFE PARSERS ----------------

def safe_float(x):
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        return float(x)
    except:
        return None

# ---------------- CONSENSUS FIX (CORE) ----------------

def save_consensus_snapshot(station="KOKC"):
    st = get_state(station)
    now = datetime.utcnow() - timedelta(hours=5)

    acc = st.get("accuracy", {}) or {}
    forecasts = st.get("forecasts", {}) or {}

    models = [m for m in acc.keys() if m != "NWS"] if acc else list(forecasts.keys())

    w_sum = 0.0
    w_total = 0.0

    pw_sum = 0.0
    pw_total = 0.0

    obs_temp = safe_float((st.get("obs") or {}).get("temperature_display"))

    debug_skipped = 0

    for model in models:
        a = acc.get(model, {}) or {}
        fcst = forecasts.get(model, {}) or {}

        raw = safe_float(fcst.get("high"))
        run_corr = (a.get("runs") or {}).get(fcst.get("run",""), {}).get("correction")
        corr = a.get("correction") if run_corr in (None,"") else run_corr

        corr = safe_float(corr)

        if raw is None or corr is None:
            debug_skipped += 1
            continue

        adj = raw + corr

        mae = safe_float(a.get("mae"))
        if mae and mae > 0:
            w = 1.0 / mae
        else:
            w = 1.0   # FIX: never drop model entirely

        w_sum += adj * w
        w_total += w

        # pace
        current_fcst = safe_float(fcst.get("current_fcst"))
        if obs_temp is not None and current_fcst is not None:
            pace = obs_temp - current_fcst
            pw_sum += pace * w
            pw_total += w

    consensus = round(w_sum / w_total, 1) if w_total > 0 else None
    cons_pace = round(pw_sum / pw_total, 2) if pw_total > 0 else None

    # 🔥 IMPORTANT FIX: do NOT silently return if null — log it
    if consensus is None:
        add_log(f"CONSENSUS NULL w_total={w_total} skipped={debug_skipped} models={len(models)}", "err", station)
        return

    entry = {
        "time": now.strftime("%H:%M"),
        "consensus": consensus,
        "pace": cons_pace,
        "obs": obs_temp
    }

    st["consensus_snapshots"].append(entry)
    st["consensus_snapshots"] = st["consensus_snapshots"][-48:]

# ---------------- STATE API ----------------

@app.route("/api/state")
def api_state():
    station = request.args.get("station","KOKC").upper()
    st = get_state(station)

    acc = st.get("accuracy", {}) or {}
    forecasts = st.get("forecasts", {}) or {}

    models = list(set(list(acc.keys()) + list(forecasts.keys())))
    models = [m for m in models if m != "NWS"]

    rows = []

    for m in models:
        a = acc.get(m, {}) or {}
        fcst = forecasts.get(m, {}) or {}

        raw = safe_float(fcst.get("high"))
        corr = safe_float(a.get("correction"))

        adj = raw + corr if raw is not None and corr is not None else None

        mae = safe_float(a.get("mae"))
        w = (1.0 / mae) if mae and mae > 0 else 1.0  # FIX

        rows.append({
            "model": m,
            "adj": adj,
            "mae": mae,
            "weight": w,
            "pace": None
        })

    w_sum = sum(r["adj"] * r["weight"] for r in rows if r["adj"] is not None)
    w_total = sum(r["weight"] for r in rows if r["adj"] is not None)

    consensus = round(w_sum / w_total, 1) if w_total > 0 else None

    pw_sum = 0
    pw_total = 0

    obs = safe_float((st.get("obs") or {}).get("temperature_display"))

    for r in rows:
        if obs is not None and r["adj"] is not None:
            pw_sum += r["weight"] * 0
            pw_total += r["weight"]

    consensus_pace = None  # simplified safe state

    return jsonify({
        "station": station,
        "consensus": consensus,
        "consensus_pace": consensus_pace,
        "rows": rows,
        "log": st["log"][:40],
        "errors": st["errors"],
        "last_updated": st["last_updated"]
    })

# ---------------- ACCURACY ----------------

@app.route("/api/accuracy", methods=["POST"])
def save_accuracy():
    station = request.args.get("station","KOKC").upper()
    st = get_state(station)
    st["accuracy"] = request.json or {}
    add_log("Accuracy updated", "ok", station)
    return jsonify({"ok": True})

# ---------------- REFRESH ----------------

@app.route("/api/refresh", methods=["POST"])
def refresh():
    return jsonify({"ok": True})

# ---------------- FRONTEND ----------------

@app.route("/")
def index():
    return "<h2>Fixed server running</h2>"

# ---------------- START ----------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT",5000)))
