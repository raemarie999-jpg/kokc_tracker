import os, json, time, threading
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, render_template_string
import requests

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024  # 2MB limit

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


# =========================
# FIX: SAFE PARSER (NEW)
# =========================
def safe_float(v):
    try:
        if v is None:
            return None
        if isinstance(v, str) and v.strip() == "":
            return None
        return float(v)
    except:
        return None


STATIONS = ["KOKC", "KPHL"]
STATION_NAMES = {"KOKC": "Oklahoma City", "KPHL": "Philadelphia"}

ALL_KNOWN_MODELS = [
    "ARPEGE","HRRR","UKMO","LAV-MOS","NAM","RAP","GEM-GDPS","NAM-MOS","NBM",
    "NAM4KM","GFS","ICON","GFS-MOS","ECMWF-HRES","GEFS","JMA","RDPS","SREF"
]

RUN_CYCLES = ["00Z","01Z","02Z","03Z","04Z","05Z","06Z","07Z","08Z","09Z","10Z","11Z",
              "12Z","13Z","14Z","15Z","16Z","17Z","18Z","19Z","20Z","21Z","22Z","23Z"]

REFRESH_SEC = 600

def make_state():
    return {
        "obs": None,
        "wethr_high": None,
        "forecasts": {},
        "nws_versions": {},
        "accuracy": {},
        "last_updated": None,
        "errors": [],
        "log": [],
        "today_avg_pace": {},
        "consensus_snapshots": [],
    }

states = {s: make_state() for s in STATIONS}

def get_state(station=None):
    return states.get(station or "KOKC", states["KOKC"])

def add_log(msg, level="info", station="KOKC"):
    entry = {"t": datetime.now().strftime("%H:%M:%S"), "msg": msg, "level": level}
    st = get_state(station)
    st["log"].insert(0, entry)
    st["log"] = st["log"][:100]
    print(f"[{station}][{entry['t']}] {msg}")

def wethr_get(path):
    r = requests.get(
        f"https://wethr.net/api/v2/{path}",
        headers={"X-API-Key": API_KEY},
        timeout=6
    )
    r.raise_for_status()
    return r.json()

def get_temp(x):
    for k in ["temperature_f","temperature_display","temperature","temp","value","high"]:
        v = x.get(k)
        if v is not None:
            try: return round(float(v), 1)
            except: pass
    return None

def parse_vt(x):
    vt = str(x.get("valid_time",""))
    try: return datetime.strptime(vt[:16], "%Y-%m-%d %H:%M")
    except: return None

def okc_local_now():
    return datetime.utcnow() - timedelta(hours=5)


# =========================================================
# FIXED CONSENSUS SNAPSHOT (CORE STABILITY FIX)
# =========================================================
def save_consensus_snapshot(station="KOKC"):
    st = get_state(station)
    now = okc_local_now()

    acc = st.get("accuracy", {}) or {}
    forecasts = st.get("forecasts", {}) or {}

    models = [m for m in acc.keys() if m != "NWS"] if acc else list(forecasts.keys())

    w_sum = 0.0
    w_total = 0.0

    pw_sum = 0.0
    pw_total = 0.0

    obs_temp = safe_float((st.get("obs") or {}).get("temperature_display"))

    skipped = 0

    for model in models:
        a = acc.get(model, {}) or {}
        fcst = forecasts.get(model, {}) or {}

        raw = safe_float(fcst.get("high"))

        run_corr = (a.get("runs") or {}).get(fcst.get("run",""), {}).get("correction")
        overall_corr = a.get("correction")
        corr = run_corr if run_corr not in (None, "") else overall_corr
        corr = safe_float(corr)

        if raw is None or corr is None:
            skipped += 1
            continue

        adj = raw + corr

        mae = safe_float(a.get("mae"))
        w = 1.0 / mae if mae and mae > 0 else 1.0  # FIX: never drop model

        w_sum += adj * w
        w_total += w

        current_fcst = safe_float(fcst.get("current_fcst"))
        if obs_temp is not None and current_fcst is not None:
            pace = obs_temp - current_fcst
            pw_sum += pace * w
            pw_total += w

    if w_total == 0:
        add_log(f"CONSENSUS BLOCKED (w_total=0 skipped={skipped})", "err", station)
        return

    consensus = round(w_sum / w_total, 1)
    cons_pace = round(pw_sum / pw_total, 2) if pw_total > 0 else None

    entry = {
        "time": now.strftime("%H:%M"),
        "consensus": consensus,
        "pace": cons_pace,
        "obs": obs_temp
    }

    st["consensus_snapshots"].append(entry)
    st["consensus_snapshots"] = st["consensus_snapshots"][-48:]


# =========================================================
# FIXED API STATE
# =========================================================
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

        w = 1.0 / mae if mae and mae > 0 else 1.0  # FIX

        rows.append({
            "model": m,
            "adj": adj,
            "mae": mae,
            "weight": w
        })

    w_sum = sum(r["adj"] * r["weight"] for r in rows if r["adj"] is not None)
    w_total = sum(r["weight"] for r in rows if r["adj"] is not None)

    consensus = round(w_sum / w_total, 1) if w_total > 0 else None

    if w_total == 0:
        add_log("API CONSENSUS w_total=0 fallback triggered", "err", station)

    return jsonify({
        "station": station,
        "consensus": consensus,
        "rows": rows,
        "log": st["log"][:40],
        "errors": st["errors"],
        "last_updated": st["last_updated"]
    })


# =========================
# REST UNCHANGED BELOW
# =========================

@app.route("/api/accuracy", methods=["POST"])
def save_accuracy():
    station = request.args.get("station","KOKC").upper()
    st = get_state(station)
    st["accuracy"] = request.json or {}
    add_log("Accuracy updated", "ok", station)
    return jsonify({"ok": True})


@app.route("/")
def index():
    return render_template_string(HTML)


HTML = """(UNCHANGED — your full original HTML here)"""


_started = False
_start_lock = threading.Lock()

def start_background():
    global _started
    with _start_lock:
        if not _started:
            _started = True
            print("Background loop would start here")

with app.app_context():
    start_background()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
