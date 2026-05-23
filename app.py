import os, json, time, threading
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, render_template_string
import requests

app = Flask(__name__)

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
STATION = "KOKC"

ALL_KNOWN_MODELS = [
    "ARPEGE","HRRR","UKMO","LAV-MOS","NAM","RAP","GEM-GDPS","NAM-MOS","NBM",
    "NAM4KM","GFS","ICON","GFS-MOS","ECMWF-HRES","GEFS","JMA","RDPS","SREF"
]
RUN_CYCLES = ["00Z","01Z","02Z","03Z","04Z","05Z","06Z","07Z","08Z","09Z","10Z","11Z",
              "12Z","13Z","14Z","15Z","16Z","17Z","18Z","19Z","20Z","21Z","22Z","23Z"]
REFRESH_SEC = 300

state = {
    "obs": None,
    "wethr_high": None,
    "forecasts": {},
    "nws_versions": {},
    "accuracy": {},
    "last_updated": None,
    "errors": [],
    "log": [],
    "today_snapshots": [],   # list of {time, model, pace} every 5 min
    "today_avg_pace": {},    # {model: avg_pace} rolling same-day average
}

def active_models():
    # Use accuracy keys if available (preserves user's ranked order), else fetch all known
    acc = state.get("accuracy", {})
    models = [m for m in acc.keys() if m != "NWS"] if acc else ALL_KNOWN_MODELS
    return models

def add_log(msg, level="info"):
    entry = {"t": datetime.now().strftime("%H:%M:%S"), "msg": msg, "level": level}
    state["log"].insert(0, entry)
    state["log"] = state["log"][:100]
    print(f"[{entry['t']}] {msg}")

def wethr_get(path):
    r = requests.get(
        f"https://wethr.net/api/v2/{path}",
        headers={"X-API-Key": API_KEY},
        timeout=8
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

def today_entries(temps):
    utc_now = datetime.utcnow()
    okc_local = utc_now - timedelta(hours=5)
    day_start = okc_local.replace(hour=0,minute=0,second=0,microsecond=0) + timedelta(hours=5)
    day_end = day_start + timedelta(hours=24)
    filtered = [x for x in temps if parse_vt(x) is not None and day_start <= parse_vt(x) < day_end]
    return filtered if filtered else temps

def fmt_run(run_raw):
    try:
        if len(run_raw) >= 13:
            return run_raw[11:13] + "Z"
        return run_raw or "—"
    except:
        return "—"

def fetch_all():
    if not API_KEY:
        add_log("No API key set", "err")
        return
    add_log("Fetching data...")
    errors = []

    # Observation
    try:
        obs = wethr_get(f"observations.php?station_code={STATION}&mode=latest")
        state["obs"] = obs
        add_log(f"Obs: {obs.get('temperature_display')}F", "ok")
    except Exception as e:
        errors.append(f"Obs: {e}")
        add_log(f"Obs error: {e}", "err")

    # Wethr high
    try:
        wh = wethr_get(f"observations.php?station_code={STATION}&mode=wethr_high&logic=nws")
        state["wethr_high"] = wh
        add_log(f"Wethr High: {wh.get('wethr_high')}F", "ok")
    except Exception as e:
        errors.append(f"WethrHigh: {e}")
        add_log(f"Wethr High error: {e}", "err")

    # Forecasts per model — always fetch all known models so data is ready before accuracy is entered
    fetch_targets = active_models() if state.get("accuracy") else ALL_KNOWN_MODELS
    utc_now = datetime.utcnow()
    for model in fetch_targets:
        try:
            data = wethr_get(f"forecasts.php?location_name={STATION}&model={requests.utils.quote(model)}&run=latest")
            temps = data if isinstance(data, list) else data.get("forecasts", [])
            meta = {} if isinstance(data, list) else data
            if temps:
                todays = today_entries(temps)
                max_entry = max(todays, key=lambda x: get_temp(x) or 0)
                raw_temp = get_temp(max_entry)
                closest = min(todays, key=lambda x: abs((parse_vt(x) - utc_now).total_seconds()) if parse_vt(x) else 99999)
                current_temp = get_temp(closest)
                run_raw = meta.get("run_time") or max_entry.get("run_time") or max_entry.get("run") or ""
                run_fmt = fmt_run(run_raw)
                state["forecasts"][model] = {
                    "high": raw_temp,
                    "current_fcst": current_temp,
                    "run": run_fmt,
                }
                add_log(f"{model}: high={raw_temp} now={current_temp} run={run_fmt} ({len(todays)} entries)", "ok")
        except Exception as e:
            errors.append(f"{model}: {e}")
            add_log(f"{model} error: {str(e)[:80]}", "warn")

    # NWS — separate endpoint
    try:
        nws_data = wethr_get(f"observations.php?station_code={STATION}&mode=nws_forecast")
        entries = nws_data if isinstance(nws_data, list) else nws_data.get("forecasts", [])
        utc_now = datetime.utcnow()
        versions = {}
        for entry in entries:
            ver = str(entry.get("version") or entry.get("forecast_version") or entry.get("run") or "current").lower()
            vt = parse_vt(entry)
            if not vt: continue
            temp_f = get_temp(entry)
            if temp_f is None: continue
            if ver not in versions: versions[ver] = []
            versions[ver].append({"vt": vt, "temp": temp_f})
        nws_out = {}
        todays_range = today_entries([{"valid_time": v["vt"].strftime("%Y-%m-%d %H:%M"), "temperature_f": v["temp"]} for vals in versions.values() for v in vals])
        for ver, pts in versions.items():
            today_pts = [p for p in pts if any(abs((p["vt"] - parse_vt(t)).total_seconds()) < 1800 for t in todays_range)] or pts
            if not today_pts: continue
            high = max(today_pts, key=lambda x: x["temp"])["temp"]
            closest = min(today_pts, key=lambda x: abs((x["vt"] - utc_now).total_seconds()))
            nws_out[ver] = {"high": high, "current_fcst": closest["temp"]}
        state["nws_versions"] = nws_out
        add_log(f"NWS: {len(nws_out)} versions", "ok")
    except Exception as e:
        add_log(f"NWS error: {str(e)[:80]}", "warn")
        state["nws_versions"] = {}

    state["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    state["errors"] = errors
    add_log(f"Done. {len(state['forecasts'])} models loaded.", "ok")

def okc_local_now():
    return datetime.utcnow() - timedelta(hours=5)

def save_pacing_snapshot(rows):
    now = okc_local_now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M")
    snapshots = load_json_file(PACING_FILE, {})
    if date_str not in snapshots:
        snapshots[date_str] = []
    entry = {"time": time_str}
    for r in rows:
        if r.get("pace") is not None:
            entry[r["model"]] = r["pace"]
    snapshots[date_str].append(entry)
    keys = sorted(snapshots.keys())
    if len(keys) > 60:
        for k in keys[:-60]:
            del snapshots[k]
    save_json_file(PACING_FILE, snapshots)
    avg = {}
    for r in rows:
        m = r["model"]
        vals = [s[m] for s in snapshots[date_str] if m in s]
        if vals:
            avg[m] = round(sum(vals)/len(vals), 2)
    state["today_avg_pace"] = avg
    add_log(f"Snapshot saved: {len([r for r in rows if r.get('pace') is not None])} models pacing, avg={list(avg.items())[:2]}", "info")

def rollup_daily_history():
    now = okc_local_now()
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    snapshots = load_json_file(PACING_FILE, {})
    if yesterday not in snapshots or not snapshots[yesterday]:
        return
    history = load_json_file(HISTORY_FILE, {})
    if yesterday in history:
        return
    day_snaps = snapshots[yesterday]
    models = set()
    for s in day_snaps:
        models.update(k for k in s.keys() if k != "time")
    daily_avg = {}
    for m in models:
        vals = [s[m] for s in day_snaps if m in s]
        if vals:
            daily_avg[m] = round(sum(vals)/len(vals), 2)
    history[yesterday] = {"avg_pace": daily_avg, "snapshot_count": len(day_snaps), "date": yesterday}
    save_json_file(HISTORY_FILE, history)
    add_log(f"Rolled up history for {yesterday} ({len(day_snaps)} snapshots)", "ok")

def build_snapshot_rows():
    acc = state["accuracy"]
    models = [m for m in acc.keys() if m != "NWS"] if acc else ALL_KNOWN_MODELS
    obs_temp = (state["obs"] or {}).get("temperature_display")
    rows = []
    for model in models:
        fcst = state["forecasts"].get(model, {})
        current_fcst = fcst.get("current_fcst")
        try:
            pace = round(float(obs_temp) - float(current_fcst), 2) if obs_temp and current_fcst else None
        except:
            pace = None
        rows.append({"model": model, "pace": pace})
    return rows

def background_loop():
    last_rollup_date = None
    while True:
        try:
            t = threading.Thread(target=fetch_all, daemon=True)
            t.start()
            t.join(timeout=90)
            if t.is_alive():
                add_log("Fetch timed out after 90s", "err")
            try:
                save_pacing_snapshot(build_snapshot_rows())
            except Exception as e:
                add_log(f"Snapshot error: {e}", "warn")
            now = okc_local_now()
            today_str = now.strftime("%Y-%m-%d")
            if now.hour == 1 and last_rollup_date != today_str:
                rollup_daily_history()
                last_rollup_date = today_str
        except Exception as e:
            add_log(f"Loop error: {e}", "err")
        time.sleep(REFRESH_SEC)

# ── Routes ────────────────────────────────────────────────────────────────────
def _get_prev_days(n):
    history = load_json_file(HISTORY_FILE, {})
    keys = sorted(history.keys(), reverse=True)[:n]
    return [{"date": k, "avg_pace": history[k]["avg_pace"], "snapshot_count": history[k].get("snapshot_count",0)} for k in keys]

@app.route("/api/state")
def api_state():
    acc = state["accuracy"]
    models = active_models()
    rows = []
    for i, model in enumerate(models):
        a = acc.get(model, {})
        fcst = state["forecasts"].get(model, {})
        raw = fcst.get("high")
        # Use run-specific correction if available, fall back to overall correction
        current_run = fcst.get("run","")  # e.g. "11Z"
        run_corr = (a.get("runs") or {}).get(current_run, {}).get("correction")
        overall_corr = a.get("correction")
        corr = run_corr if (run_corr not in (None,"")) else overall_corr
        try: adj = round(float(raw) + float(corr), 1) if raw is not None and corr not in (None,"") else None
        except: adj = None
        obs_temp = (state["obs"] or {}).get("temperature_display")
        current_fcst = fcst.get("current_fcst")
        try: pace = round(float(obs_temp) - float(current_fcst), 1) if obs_temp and current_fcst else None
        except: pace = None
        rows.append({
            "rank": i+1, "model": model,
            "run": fcst.get("run","—"),
            "raw_high": raw, "correction": corr,
            "corr_source": "run" if (run_corr not in (None,"")) else "overall",
            "adj_high": adj, "pace": pace,
            "mae": a.get("mae"), "rmse": a.get("rmse"),
            "runs": a.get("runs", {}),
        })
    w_sum, w_total = 0, 0
    for r in rows:
        try:
            mae = float(r["mae"]); adj = r["adj_high"] if r["adj_high"] is not None else r["raw_high"]
            if mae > 0 and adj is not None:
                w = 1/mae; w_sum += adj*w; w_total += w
        except: pass
    consensus = round(w_sum/w_total, 1) if w_total > 0 else None
    return jsonify({
        "obs": state["obs"], "wethr_high": state["wethr_high"],
        "rows": rows, "consensus": consensus,
        "last_updated": state["last_updated"], "errors": state["errors"],
        "log": state["log"][:30], "models": active_models(),
        "nws_versions": state["nws_versions"],
        "today_avg_pace": state["today_avg_pace"],
        "today_snapshot_count": len(load_json_file(PACING_FILE, {}).get(okc_local_now().strftime("%Y-%m-%d"), [])),
        "prev_days": _get_prev_days(3),
    })

@app.route("/api/history")
def api_history():
    history = load_json_file(HISTORY_FILE, {})
    return jsonify(history)

@app.route("/api/accuracy", methods=["POST"])
def save_accuracy():
    state["accuracy"] = request.json or {}
    add_log("Accuracy data updated", "ok")
    return jsonify({"ok": True})

@app.route("/api/refresh", methods=["POST"])
def manual_refresh():
    threading.Thread(target=fetch_all, daemon=True).start()
    return jsonify({"ok": True})

@app.route("/")
def index():
    return render_template_string(HTML)

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>KOKC Model Tracker</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600;700&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#080c10;--bg2:#0e1520;--bg3:#0b1118;--border:#1a2535;
  --text:#c9d4e0;--dim:#4a6080;--dimmer:#2a3a50;
  --blue:#38bdf8;--green:#4ade80;--yellow:#facc15;--red:#f87171;--purple:#c084fc;
}
body{background:var(--bg);color:var(--text);font-family:'IBM Plex Mono',monospace;font-size:13px;min-height:100vh}
header{background:var(--bg3);border-bottom:1px solid var(--border);padding:14px 20px;
  position:sticky;top:0;z-index:20;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px}
h1{font-size:18px;color:#e8f0f8;letter-spacing:-.5px}
.sub{font-size:10px;color:var(--dim);letter-spacing:2px;text-transform:uppercase;margin-top:2px}
.hright{display:flex;align-items:center;gap:16px;flex-wrap:wrap}
.sp{width:1px;height:40px;background:var(--border)}
.stat-pill .lbl{font-size:9px;color:var(--dim);letter-spacing:2px;text-transform:uppercase}
.stat-pill .val{font-size:22px;font-weight:700;line-height:1.1}
.stat-pill .sub2{font-size:9px;color:var(--dimmer)}
nav{display:flex;gap:2px;background:var(--bg3);border-bottom:1px solid var(--border);padding:0 20px}
nav button{background:none;border:none;border-bottom:2px solid transparent;color:var(--dim);
  padding:10px 16px;font-size:11px;letter-spacing:1.5px;text-transform:uppercase;
  cursor:pointer;font-family:inherit;transition:color .15s}
nav button.active{border-bottom-color:var(--blue);color:var(--blue)}
main{padding:20px;max-width:1100px;margin:0 auto}
.tab{display:none}.tab.active{display:block}
.card{background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:16px 18px;margin-bottom:16px}
.ctitle{font-size:10px;letter-spacing:2.5px;color:var(--blue);text-transform:uppercase;margin-bottom:12px}
.srow{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:16px}
.sc{background:#0b1520;border:1px solid var(--border);border-radius:6px;padding:12px 16px;flex:1;min-width:120px}
.sc .lbl{font-size:9px;letter-spacing:2px;color:var(--dim);text-transform:uppercase}
.sc .v{font-size:22px;font-weight:700;margin-top:4px;line-height:1}
.sc .s{font-size:10px;color:var(--dimmer);margin-top:3px}
table{width:100%;border-collapse:collapse}
th{padding:7px 10px;text-align:left;font-size:10px;letter-spacing:1.5px;color:var(--dim);
   text-transform:uppercase;border-bottom:1px solid var(--border);white-space:nowrap}
td{padding:8px 10px;border-bottom:1px solid #111922;white-space:nowrap}
tr:nth-child(even) td{background:#0a1018}
input[type=number]{background:var(--bg);border:1px solid #1e2e42;border-radius:4px;
  color:var(--text);padding:4px 8px;font-size:12px;width:70px;font-family:inherit;outline:none}
input[type=number]:focus{border-color:var(--blue)}
.btn{background:none;border:1px solid var(--blue);color:var(--blue);border-radius:4px;
  padding:6px 14px;font-size:11px;letter-spacing:1px;cursor:pointer;text-transform:uppercase;font-family:inherit}
.btn-red{border-color:var(--red);color:var(--red)}
.btn-green{border-color:var(--green);color:var(--green)}
.dot{width:7px;height:7px;border-radius:50%;display:inline-block;margin-right:5px}
.dot-green{background:var(--green);box-shadow:0 0 6px var(--green)}
.dot-red{background:var(--red);box-shadow:0 0 6px var(--red)}
.dot-yellow{background:var(--yellow);box-shadow:0 0 6px var(--yellow)}
.pbars{display:flex;flex-direction:column;gap:7px}
.prow{display:flex;align-items:center;gap:10px}
.plabel{width:80px;font-size:11px;color:#8aabcc}
.pbar{height:10px;border-radius:3px}
.logbox{background:#060a0e;border-radius:4px;padding:12px;max-height:400px;overflow-y:auto}
.pill-y{background:#facc1522;color:var(--yellow);border-radius:3px;padding:2px 7px;font-size:10px;font-weight:600}
.pill-g{background:#4ade8022;color:var(--green);border-radius:3px;padding:2px 7px;font-size:10px;font-weight:600}
</style>
</head>
<body>
<header>
  <div>
    <h1>KOKC &middot; Model Tracker</h1>
    <div class="sub">Oklahoma City Will Rogers World Airport</div>
  </div>
  <div class="hright">
    <div class="stat-pill">
      <div class="lbl">Live Obs</div>
      <div class="val" id="h-obs" style="color:var(--yellow)">--</div>
      <div class="sub2" id="h-obs-t">awaiting</div>
    </div>
    <div class="sp"></div>
    <div class="stat-pill">
      <div class="lbl">Wethr High</div>
      <div class="val" id="h-wh" style="color:var(--green)">--</div>
      <div class="sub2">NWS logic</div>
    </div>
    <div class="sp"></div>
    <div class="stat-pill">
      <div class="lbl">Consensus</div>
      <div class="val" id="h-con" style="color:var(--blue)">--</div>
      <div class="sub2">MAE-weighted</div>
    </div>
    <div class="sp"></div>
    <div style="text-align:right">
      <div style="display:flex;align-items:center;gap:6px;font-size:10px;color:var(--dim)">
        <span class="dot dot-yellow" id="sdot"></span><span id="stxt">Loading...</span>
      </div>
      <div style="font-size:10px;color:var(--dimmer);margin-top:3px">Next: <span id="cnt">5:00</span></div>
      <button class="btn" style="margin-top:4px;padding:3px 10px;font-size:10px" onclick="manualRefresh()">&#8635; NOW</button>
    </div>
  </div>
</header>

<nav>
  <button class="active" onclick="showTab('dashboard',this)">&#128202; Dashboard</button>
  <button onclick="showTab('entry',this)">&#9728;&#65039; Morning Entry</button>
  <button onclick="showTab('runs',this)">&#128336; Run Accuracy</button>
  <button onclick="showTab('log',this)">&#128319; Log</button>
  <button onclick="showTab('history',this)">&#128196; History</button>
</nav>

<main>

<!-- DASHBOARD -->
<div class="tab active" id="tab-dashboard">
  <div class="srow">
    <div class="sc"><div class="lbl">Current Temp</div><div class="v" id="s-obs" style="color:var(--yellow)">--</div><div class="s" id="s-obs-t">awaiting</div></div>
    <div class="sc"><div class="lbl">Wethr High</div><div class="v" id="s-wh" style="color:var(--green)">--</div><div class="s">NWS trading day</div></div>
    <div class="sc"><div class="lbl">Consensus High</div><div class="v" id="s-con" style="color:var(--blue)">--</div><div class="s">MAE-weighted adj</div></div>
    <div class="sc"><div class="lbl">Models Live</div><div class="v" id="s-mods" style="color:var(--purple)">--</div><div class="s">forecast runs</div></div>
  </div>

  <div class="card">
    <div class="ctitle">
      Top 10 Models &mdash; Live Forecasts + Accuracy Adjustments
      <span class="pill-y" id="acc-badge" style="display:none">Enter accuracy in Morning Entry</span>
      <span class="pill-g" id="acc-loaded" style="display:none">Accuracy loaded</span>
    </div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr><th>#</th><th>Model</th><th>Run</th><th>Fcst High</th><th>Correction</th><th>Adj High</th><th>Obs Pace</th><th>MAE</th><th>RMSE</th></tr></thead>
        <tbody id="main-tbody"></tbody>
      </table>
    </div>
  </div>

  <div class="card" id="pace-card" style="display:none">
    <div class="ctitle">Model Pacing vs Current Obs (<span id="pace-obs">--</span>F)</div>
    <div class="pbars" id="pbars"></div>
    <div style="font-size:10px;color:var(--dimmer);margin-top:10px">Pace = current obs minus model forecast for this hour</div>
  </div>

  <div class="card" id="avg-pace-card" style="display:none">
    <div class="ctitle">Today's Rolling Average Pace (since 1AM)</div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr><th>Model</th><th>Avg Pace</th><th>Snapshots</th></tr></thead>
        <tbody id="avg-pace-tbody"></tbody>
      </table>
    </div>
  </div>

  <div class="card" id="prev-days-card" style="display:none">
    <div class="ctitle">Previous 3 Days Average Pace</div>
    <div style="overflow-x:auto"><table><thead id="prev-days-thead"></thead><tbody id="prev-days-tbody"></tbody></table></div>
  </div>

  <div class="card" id="nws-card" style="display:none">
    <div class="ctitle">NWS Forecast Versions</div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr><th>Version</th><th>Fcst High</th><th>Adj High</th><th>Current Fcst</th><th>Obs Pace</th></tr></thead>
        <tbody id="nws-tbody"></tbody>
      </table>
    </div>
  </div>
</div>

<!-- MORNING ENTRY -->
<div class="tab" id="tab-entry">
  <div class="card" style="border-color:#1e3a5f">
    <div class="ctitle">Fast Import &mdash; Paste JSON from Claude</div>
    <p style="color:var(--dim);font-size:12px;line-height:1.7;margin-bottom:12px">
      Each morning: screenshot accuracy tables, send to Claude, paste JSON here.
    </p>
    <textarea id="json-paste" placeholder="Paste JSON here..." style="width:100%;height:110px;background:#060a0e;border:1px solid #1e3a5f;border-radius:4px;color:var(--text);padding:10px;font-family:inherit;font-size:11px;resize:vertical;outline:none"></textarea>
    <div style="display:flex;gap:10px;align-items:center;margin-top:10px;flex-wrap:wrap">
      <button class="btn" onclick="loadFromJSON()">Load JSON</button>
      <span style="font-size:10px;color:var(--dim)" id="json-status"></span>
    </div>
  </div>

  <details style="margin-bottom:16px">
    <summary style="cursor:pointer;color:var(--dim);font-size:11px;letter-spacing:1px;padding:10px 0;list-style:none">&#9658; Manual entry (fallback)</summary>
    <div style="margin-top:12px">
      <div class="card">
        <div class="ctitle">Overall 7D Accuracy</div>
        <div style="overflow-x:auto">
          <table><thead><tr><th>Model</th><th>MAE</th><th>Correction</th><th>RMSE</th></tr></thead><tbody id="ov-tbody"></tbody></table>
        </div>
      </div>
      <div class="card">
        <div class="ctitle">Run-Specific Corrections</div>
        <div style="overflow-x:auto"><table><thead><tr><th>Model</th><th>00Z</th><th>03Z</th><th>06Z</th><th>09Z</th><th>12Z</th><th>15Z</th><th>18Z</th><th>21Z</th></tr></thead><tbody id="run-tbody"></tbody></table></div>
        <div style="margin-top:14px;display:flex;gap:10px;align-items:center;flex-wrap:wrap">
          <button class="btn btn-green" onclick="saveAccuracy()">Save</button>
          <button class="btn btn-red" onclick="clearAccuracy()">Clear All</button>
          <span style="font-size:10px;color:var(--dim)" id="save-status"></span>
        </div>
      </div>
    </div>
  </details>

  <div class="card" id="acc-preview" style="display:none">
    <div class="ctitle">Currently Loaded</div>
    <div style="overflow-x:auto"><table><thead><tr><th>Model</th><th>MAE</th><th>Correction</th><th>RMSE</th><th>Runs</th></tr></thead><tbody id="prev-tbody"></tbody></table></div>
    <div style="margin-top:10px;display:flex;gap:10px;align-items:center">
      <button class="btn btn-red" onclick="clearAccuracy()">Clear All</button>
      <span style="font-size:10px;color:var(--dim)" id="acc-loaded-time"></span>
    </div>
  </div>
</div>

<!-- RUN ACCURACY -->
<div class="tab" id="tab-runs">
  <div class="card">
    <div class="ctitle">Run-Specific Accuracy</div>
    <div style="overflow-x:auto"><table><thead><tr><th>Model</th><th>00Z</th><th>03Z</th><th>06Z</th><th>09Z</th><th>12Z</th><th>15Z</th><th>18Z</th><th>21Z</th></tr></thead><tbody id="runview-tbody"></tbody></table></div>
    <div class="ctitle" style="margin-top:20px">Current Run per Model</div>
    <div style="display:flex;flex-wrap:wrap;gap:8px;margin-top:8px" id="run-cards"></div>
  </div>
</div>

<!-- LOG -->
<div class="tab" id="tab-log">
  <div class="card">
    <div class="ctitle">Fetch Log</div>
    <div class="logbox" id="logbox"><div style="color:var(--dimmer)">No entries yet.</div></div>
  </div>
</div>

<!-- HISTORY -->
<div class="tab" id="tab-history">
  <div class="card">
    <div class="ctitle">Daily Pacing History</div>
    <p style="color:var(--dim);font-size:11px;margin-bottom:12px">Average pace per model for each completed day. Positive = ran warmer than model forecast.</p>
    <div style="overflow-x:auto"><table><thead id="hist-thead"></thead><tbody id="hist-tbody"></tbody></table></div>
    <div style="font-size:10px;color:var(--dimmer);margin-top:10px" id="hist-count"></div>
  </div>
</div>

</main>

<script>
var MODELS = [];
var accData = {};
try { accData = JSON.parse(localStorage.getItem("kokc_acc") || "{}"); } catch(e){}
if(Object.keys(accData).length) MODELS = Object.keys(accData).filter(function(m){ return m !== "NWS"; });
var countdown = 300;
var countdownTimer;

var MANUAL_RUNS = ["00Z","03Z","06Z","09Z","12Z","15Z","18Z","21Z"];

function fmt1(v){ return (v==null||v==="") ? "--" : Number(v).toFixed(1); }
function fmtC(v){
  if(v==null||v==="") return "--";
  var n=Number(v); return (n>=0?"+":"")+n.toFixed(1)+"F";
}
function corrColor(v){
  if(v==null||v==="") return "var(--dim)";
  return Number(v)>0?"#60a5fa":Number(v)<0?"#f87171":"var(--dim)";
}
function maeColor(v){
  if(v==null||v==="") return "var(--dim)";
  var n=Number(v); return n<=1?"var(--green)":n<=2?"var(--yellow)":"var(--red)";
}
function paceColor(v){
  var n=Math.abs(Number(v)); return n<=1?"var(--green)":n<=3?"var(--yellow)":"var(--red)";
}

function showTab(id,btn){
  document.querySelectorAll(".tab").forEach(function(t){t.classList.remove("active");});
  document.querySelectorAll("nav button").forEach(function(b){b.classList.remove("active");});
  document.getElementById("tab-"+id).classList.add("active");
  btn.classList.add("active");
}

function buildForms(){
  var ov = document.getElementById("ov-tbody");
  if(!ov) return;
  var mods = MODELS.length ? MODELS : ["HRRR","ARPEGE","NAM","UKMO","LAV-MOS","RAP","GEM-GDPS","NAM-MOS","NBM","NAM4KM"];
  ov.innerHTML = mods.map(function(m,i){
    var a = accData[m]||{};
    var bg = i%2?"background:#0a1018":"";
    return '<tr style="'+bg+'"><td style="color:#e8f0f8;font-weight:600">'+m+'</td>'
      +'<td><input type="number" step="0.1" placeholder="0.0" id="ov-mae-'+m+'" value="'+(a.mae||"")+'"></td>'
      +'<td><input type="number" step="0.1" placeholder="0.0" id="ov-corr-'+m+'" value="'+(a.correction||"")+'"></td>'
      +'<td><input type="number" step="0.1" placeholder="0.0" id="ov-rmse-'+m+'" value="'+(a.rmse||"")+'"></td></tr>';
  }).join("");
  var rb = document.getElementById("run-tbody");
  rb.innerHTML = mods.map(function(m,i){
    var a = accData[m]||{};
    var bg = i%2?"background:#0a1018":"";
    var cells = MANUAL_RUNS.map(function(r){
      var rd = (a.runs||{})[r]||{};
      return '<td style="padding:5px 6px"><div style="display:flex;flex-direction:column;gap:3px">'
        +'<input type="number" step="0.1" placeholder="MAE" style="width:56px;font-size:11px" id="rm-mae-'+m+'-'+r+'" value="'+(rd.mae||"")+'"><br>'
        +'<input type="number" step="0.1" placeholder="Corr" style="width:56px;font-size:11px" id="rm-corr-'+m+'-'+r+'" value="'+(rd.correction||"")+'"></div></td>';
    }).join("");
    return '<tr style="'+bg+'"><td style="color:#8aabcc;font-weight:600">'+m+'</td>'+cells+'</tr>';
  }).join("");
}

function loadFromJSON(){
  var raw = document.getElementById("json-paste").value.trim();
  var status = document.getElementById("json-status");
  if(!raw){ status.style.color="var(--red)"; status.textContent="Nothing to paste."; return; }
  try {
    var parsed = JSON.parse(raw);
    var keys = Object.keys(parsed);
    if(!keys.length){ status.style.color="var(--red)"; status.textContent="No models found."; return; }
    accData = parsed;
    MODELS = keys.filter(function(m){ return m !== "NWS"; });
    localStorage.setItem("kokc_acc", JSON.stringify(parsed));
    localStorage.setItem("kokc_acc_time", new Date().toLocaleString());
    fetch("/api/accuracy",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(parsed)})
      .then(function(){
        status.style.color="var(--green)";
        status.textContent = "Loaded "+keys.length+" models at "+new Date().toLocaleTimeString();
        document.getElementById("json-paste").value="";
        buildForms(); renderPreview(); poll();
      }).catch(function(){ status.style.color="var(--red)"; status.textContent="Server save failed."; });
  } catch(e) {
    status.style.color="var(--red)"; status.textContent="Invalid JSON: "+e.message;
  }
}

function saveAccuracy(){
  var mods = MODELS.length ? MODELS : [];
  var data = {};
  mods.forEach(function(m){
    data[m] = {
      mae: document.getElementById("ov-mae-"+m) ? document.getElementById("ov-mae-"+m).value : "",
      correction: document.getElementById("ov-corr-"+m) ? document.getElementById("ov-corr-"+m).value : "",
      rmse: document.getElementById("ov-rmse-"+m) ? document.getElementById("ov-rmse-"+m).value : "",
      runs: {}
    };
    MANUAL_RUNS.forEach(function(r){
      var mae_el = document.getElementById("rm-mae-"+m+"-"+r);
      var corr_el = document.getElementById("rm-corr-"+m+"-"+r);
      data[m].runs[r] = {
        mae: mae_el ? mae_el.value : "",
        correction: corr_el ? corr_el.value : ""
      };
    });
  });
  accData = data;
  localStorage.setItem("kokc_acc", JSON.stringify(data));
  fetch("/api/accuracy",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(data)})
    .then(function(){ document.getElementById("save-status").textContent="Saved "+new Date().toLocaleTimeString(); });
}

function clearAccuracy(){
  if(!confirm("Clear all accuracy data?")) return;
  accData = {}; MODELS = [];
  localStorage.removeItem("kokc_acc"); localStorage.removeItem("kokc_acc_time");
  fetch("/api/accuracy",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({})});
  buildForms(); renderPreview();
  document.getElementById("save-status").textContent="Cleared";
}

function renderPreview(){
  var hasAny = Object.keys(accData).some(function(m){ return accData[m] && accData[m].mae; });
  var el = document.getElementById("acc-preview");
  document.getElementById("acc-badge").style.display = hasAny ? "none" : "inline";
  document.getElementById("acc-loaded").style.display = hasAny ? "inline" : "none";
  if(!hasAny){ el.style.display="none"; return; }
  el.style.display="block";
  var t = localStorage.getItem("kokc_acc_time");
  if(t) document.getElementById("acc-loaded-time").textContent="Loaded: "+t;
  var mods = Object.keys(accData);
  document.getElementById("prev-tbody").innerHTML = mods.map(function(m,i){
    var a = accData[m]||{};
    var runs = Object.entries(a.runs||{}).filter(function(e){ return e[1].mae||e[1].correction; }).map(function(e){ return e[0]; }).join(", ")||"--";
    var bg = i%2?"background:#0a1018":"";
    return '<tr style="'+bg+'">'
      +'<td style="color:#e8f0f8;font-weight:600">'+m+'</td>'
      +'<td style="color:'+maeColor(a.mae)+'">'+(a.mae?fmt1(a.mae)+"F":"--")+'</td>'
      +'<td style="color:'+corrColor(a.correction)+'">'+(a.correction!=null&&a.correction!==""?fmtC(a.correction):"--")+'</td>'
      +'<td style="color:var(--dim)">'+(a.rmse?fmt1(a.rmse)+"F":"--")+'</td>'
      +'<td style="color:var(--dim);font-size:11px">'+runs+'</td></tr>';
  }).join("");
}

function render(data){
  if(data.models && data.models.length) MODELS = data.models.filter(function(m){ return m!=="NWS"; });
  var obs = data.obs;
  var wh = data.wethr_high;
  var rows = data.rows||[];
  var con = data.consensus;

  if(obs){
    var t = obs.temperature_display;
    document.getElementById("h-obs").textContent = t+"F";
    document.getElementById("s-obs").textContent = t+"F";
    var ot = (obs.observation_time||"").slice(11,16)||"--";
    document.getElementById("h-obs-t").textContent = ot;
    document.getElementById("s-obs-t").textContent = ot;
    document.getElementById("pace-obs").textContent = t;
  }
  if(wh){ document.getElementById("h-wh").textContent=wh.wethr_high+"F"; document.getElementById("s-wh").textContent=wh.wethr_high+"F"; }
  if(con){ document.getElementById("h-con").textContent=con+"F"; document.getElementById("s-con").textContent=con+"F"; }
  document.getElementById("s-mods").textContent = rows.filter(function(r){ return r.raw_high!=null; }).length+"/"+rows.length;

  // Main table
  document.getElementById("main-tbody").innerHTML = rows.map(function(r,i){
    var bg = i%2?"background:#0a1018":"";
    return '<tr style="'+bg+'">'
      +'<td style="color:var(--dim)">#'+r.rank+'</td>'
      +'<td style="color:#e8f0f8;font-weight:600">'+r.model+'</td>'
      +'<td style="color:var(--dim);font-size:11px">'+(r.run||"--")+'</td>'
      +'<td style="color:var(--yellow)">'+(r.raw_high!=null?r.raw_high+"F":"--")+'</td>'
      +'<td style="color:'+corrColor(r.correction)+'">'+(r.correction!=null&&r.correction!==""?fmtC(r.correction)+(r.corr_source==="run"?' <span style="font-size:9px;color:#38bdf8" title="Run-specific correction">R</span>':''):"--")+'</td>'
      +'<td style="color:var(--green);font-weight:600">'+(r.adj_high!=null?r.adj_high+"F":"--")+'</td>'
      +'<td style="color:'+(r.pace!=null?paceColor(r.pace):"#1e2e42")+'">'+(r.pace!=null?(r.pace>=0?"+":"")+r.pace+"F":"--")+'</td>'
      +'<td style="color:'+maeColor(r.mae)+'">'+(r.mae?fmt1(r.mae)+"F":"--")+'</td>'
      +'<td style="color:var(--dim)">'+(r.rmse?fmt1(r.rmse)+"F":"--")+'</td></tr>';
  }).join("");

  // Pacing bars
  var paceRows = rows.filter(function(r){ return r.pace!=null; });
  if(paceRows.length && obs){
    document.getElementById("pace-card").style.display="block";
    document.getElementById("pbars").innerHTML = paceRows.map(function(r){
      var p=Number(r.pace); var w=Math.min(Math.abs(p)*14,140);
      var col=p>=0?"var(--green)":"var(--red)";
      return '<div class="prow"><div class="plabel">'+r.model+'</div>'
        +'<div style="width:160px"><div class="pbar" style="width:'+w+'px;background:'+col+'33;border:1px solid '+col+'"></div></div>'
        +'<span style="font-size:11px;color:'+paceColor(r.pace)+';font-weight:600">'+(p>=0?"+":"")+r.pace+'F</span></div>';
    }).join("");
  }

  // NWS versions
  var nws = data.nws_versions||{};
  var nwsKeys = Object.keys(nws);
  var nwsCard = document.getElementById("nws-card");
  if(nwsKeys.length){
    nwsCard.style.display="block";
    var nwsAcc = accData["NWS"]||{};
    var nwsCorr = (nwsAcc.correction!=null&&nwsAcc.correction!=="") ? Number(nwsAcc.correction) : null;
    var obsT = obs ? Number(obs.temperature_display) : null;
    nwsKeys.sort(function(a,b){
      if(a==="current") return -1; if(b==="current") return 1;
      return (parseInt(b.replace("v",""))||0)-(parseInt(a.replace("v",""))||0);
    });
    document.getElementById("nws-tbody").innerHTML = nwsKeys.map(function(ver,i){
      var v = nws[ver];
      var adj = (v.high!=null&&nwsCorr!=null) ? (v.high+nwsCorr).toFixed(1) : null;
      var pace = (obsT!=null&&v.current_fcst!=null) ? (obsT-v.current_fcst).toFixed(1) : null;
      var vc = ver==="current"?"var(--green)":"var(--blue)";
      var vl = ver==="current"?"Current":ver.toUpperCase();
      var pc = pace!=null?paceColor(pace):"#1e2e42";
      var ps = pace!=null?(Number(pace)>=0?"+":"")+pace+"F":"--";
      var bg = i%2?"background:#0a1018":"";
      return '<tr style="'+bg+'">'
        +'<td style="color:'+vc+';font-weight:600">'+vl+'</td>'
        +'<td style="color:var(--yellow)">'+(v.high!=null?v.high+"F":"--")+'</td>'
        +'<td style="color:var(--green)">'+(adj?adj+"F":"--")+'</td>'
        +'<td style="color:#94a3b8">'+(v.current_fcst!=null?v.current_fcst+"F":"--")+'</td>'
        +'<td style="color:'+pc+'">'+ps+'</td></tr>';
    }).join("");
  } else {
    nwsCard.style.display="none";
  }

  // Run accuracy tab
  document.getElementById("runview-tbody").innerHTML = rows.map(function(r,i){
    var bg = i%2?"background:#0a1018":"";
    var cells = MANUAL_RUNS.map(function(run){
      var rd = (r.runs||{})[run]||{};
      var has = rd.mae||rd.correction;
      return '<td style="text-align:center">'+(has
        ?'<div style="line-height:1.8">'+(rd.mae?'<div style="color:'+maeColor(rd.mae)+'">'+fmt1(rd.mae)+'F</div>':'')+
          (rd.correction!=null&&rd.correction!==""?'<div style="color:'+corrColor(rd.correction)+'">'+fmtC(rd.correction)+'</div>':'')+'</div>'
        :'<span style="color:#1e2e42">--</span>')+'</td>';
    }).join("");
    return '<tr style="'+bg+'"><td style="color:#e8f0f8;font-weight:600">'+r.model+'</td>'+cells+'</tr>';
  }).join("");

  // Run cards
  document.getElementById("run-cards").innerHTML = rows.map(function(r){
    var runKey = r.run ? r.run.replace(/[^0-9]/g,"").slice(0,2)+"Z" : "";
    var rd = (r.runs||{})[runKey]||{};
    var hasC = rd.correction!=null&&rd.correction!=="";
    return '<div style="background:#0b1520;border:1px solid var(--border);border-radius:5px;padding:8px 12px;min-width:120px">'
      +'<div style="font-size:11px;color:#8aabcc;font-weight:600">'+r.model+'</div>'
      +'<div style="font-size:13px;color:var(--blue);margin-top:2px">'+(r.run||"--")+'</div>'
      +(hasC?'<div style="font-size:11px;color:'+corrColor(rd.correction)+';margin-top:2px">Corr: '+fmtC(rd.correction)+'</div>'
             :'<div style="font-size:10px;color:#2a4060;margin-top:2px">No run corr</div>')
      +'</div>';
  }).join("");

  // Log
  if(data.log&&data.log.length){
    document.getElementById("logbox").innerHTML = data.log.map(function(e){
      var col = e.level==="ok"?"var(--green)":e.level==="err"?"var(--red)":e.level==="warn"?"var(--yellow)":"var(--dim)";
      return '<div style="margin-bottom:5px"><span style="color:var(--dimmer)">['+e.t+']</span> <span style="color:'+col+'">'+e.msg+'</span></div>';
    }).join("");
  }

  // Today avg pace
  var avgPace = data.today_avg_pace || {};
  var avgModels = Object.keys(avgPace);
  var avgCard = document.getElementById("avg-pace-card");
  var todaySnaps = data.today_snapshot_count || 0;
  if(avgModels.length){
    avgCard.style.display="block";
    document.getElementById("avg-pace-tbody").innerHTML = avgModels.map(function(m,i){
      var p = avgPace[m];
      var bg = i%2?"background:#0a1018":"";
      var pc = paceColor(p);
      return '<tr style="'+bg+'"><td style="color:#e8f0f8;font-weight:600">'+m+'</td>'
        +'<td style="color:'+pc+';font-weight:600">'+(p>=0?"+":"")+p.toFixed(2)+'F</td>'
        +'<td style="color:var(--dim)">'+todaySnaps+'</td></tr>';
    }).join("");
  } else { avgCard.style.display="none"; }

  // Prev 3 days
  var prevDays = data.prev_days || [];
  var prevCard = document.getElementById("prev-days-card");
  if(prevDays.length){
    prevCard.style.display="block";
    var allModels = [];
    prevDays.forEach(function(d){ Object.keys(d.avg_pace).forEach(function(m){ if(!allModels.includes(m)) allModels.push(m); }); });
    document.getElementById("prev-days-thead").innerHTML = '<tr><th>Model</th>'+prevDays.map(function(d){ return '<th>'+d.date.slice(5)+'</th>'; }).join("")+'</tr>';
    document.getElementById("prev-days-tbody").innerHTML = allModels.map(function(m,i){
      var bg = i%2?"background:#0a1018":"";
      var cells = prevDays.map(function(d){
        var p = d.avg_pace[m];
        if(p==null) return '<td style="color:#1e2e42">--</td>';
        return '<td style="color:'+paceColor(p)+';font-weight:600">'+(p>=0?"+":"")+p.toFixed(2)+'F</td>';
      }).join("");
      return '<tr style="'+bg+'"><td style="color:#e8f0f8;font-weight:600">'+m+'</td>'+cells+'</tr>';
    }).join("");
  } else { prevCard.style.display="none"; }

  // Status
  document.getElementById("sdot").className = "dot "+(data.errors&&data.errors.length?"dot-yellow":"dot-green");
  document.getElementById("stxt").textContent = data.last_updated?"Updated "+data.last_updated.slice(11,16):"Live";
}

function poll(){
  if(Object.keys(accData).length){
    fetch("/api/accuracy",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(accData)});
  }
  fetch("/api/state").then(function(r){ return r.json(); }).then(render).catch(function(e){ console.error(e); });
}

function manualRefresh(){
  fetch("/api/refresh",{method:"POST"});
  countdown=300;
  setTimeout(poll,3000);
}

function startCountdown(){
  clearInterval(countdownTimer);
  countdown=300;
  countdownTimer=setInterval(function(){
    countdown=Math.max(0,countdown-1);
    var m=Math.floor(countdown/60); var s=String(countdown%60).padStart(2,"0");
    document.getElementById("cnt").textContent=m+":"+s;
    if(countdown===0){ poll(); countdown=300; }
  },1000);
}

buildForms(); renderPreview(); poll(); startCountdown(); setInterval(poll,300000);

function loadHistory(){
  fetch("/api/history").then(function(r){ return r.json(); }).then(function(history){
    var dates = Object.keys(history).sort().reverse();
    var thead = document.getElementById("hist-thead");
    var tbody = document.getElementById("hist-tbody");
    var countEl = document.getElementById("hist-count");
    if(!dates.length){
      tbody.innerHTML = '<tr><td colspan="2" style="color:var(--dim)">No history yet. Data accumulates after the first full day.</td></tr>';
      return;
    }
    var allModels = [];
    dates.forEach(function(d){ Object.keys(history[d].avg_pace).forEach(function(m){ if(!allModels.includes(m)) allModels.push(m); }); });
    thead.innerHTML = '<tr><th>Model</th>'+dates.map(function(d){ return '<th>'+d+'</th>'; }).join("")+'</tr>';
    tbody.innerHTML = allModels.map(function(m,i){
      var bg = i%2?"background:#0a1018":"";
      var cells = dates.map(function(d){
        var p = history[d].avg_pace[m];
        if(p==null) return '<td style="color:#1e2e42">--</td>';
        return '<td style="color:'+paceColor(p)+';font-weight:600">'+(p>=0?"+":"")+p.toFixed(2)+'F</td>';
      }).join("");
      return '<tr style="'+bg+'"><td style="color:#e8f0f8;font-weight:600">'+m+'</td>'+cells+'</tr>';
    }).join("");
    countEl.textContent = dates.length+" days stored";
  }).catch(function(e){ console.error("History load error",e); });
}

// Load history when tab is clicked
document.querySelectorAll("nav button").forEach(function(btn){
  btn.addEventListener("click", function(){
    if(btn.textContent.includes("History")) loadHistory();
  });
});
</script>
</body>
</html>
"""

_started = False
_start_lock = threading.Lock()

def start_background():
    global _started
    with _start_lock:
        if not _started:
            _started = True
            t = threading.Thread(target=background_loop, daemon=True)
            t.start()

with app.app_context():
    start_background()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)





















