import os, json, time, threading
from datetime import datetime
from flask import Flask, jsonify, request, render_template_string
import requests

app = Flask(__name__)

API_KEY = os.environ.get("WETHR_API_KEY", "")
STATION = "KOKC"
ALL_KNOWN_MODELS = [
    "ARPEGE","HRRR","UKMO","LAV-MOS","NAM","RAP","GEM-GDPS","NWS","NAM-MOS","NBM",
    "NAM4KM","GFS","ICON","GFS-MOS","ECMWF-HRES","GEFS","JMA","RDPS","SREF","NBM-ENS"
]
RUN_CYCLES = ["00Z","01Z","02Z","03Z","04Z","05Z","06Z","07Z","08Z","09Z","10Z","11Z",
              "12Z","13Z","14Z","15Z","16Z","17Z","18Z","19Z","20Z","21Z","22Z","23Z"]

def active_models():
    """Return today's ranked models from accuracy data, or fall back to known list."""
    acc = state.get("accuracy", {})
    if acc:
        return list(acc.keys())
    return ALL_KNOWN_MODELS[:10]
REFRESH_SEC = 300  # 5 minutes

# In-memory state
state = {
    "obs": None,
    "wethr_high": None,
    "forecasts": {},
    "accuracy": {},
    "last_updated": None,
    "errors": [],
    "log": []
}

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

def fetch_all():
    if not API_KEY:
        add_log("No API key set", "err")
        return
    add_log("Fetching data...")
    errors = []

    # Latest observation
    try:
        obs = wethr_get(f"observations.php?station_code={STATION}&mode=latest")
        state["obs"] = obs
        add_log(f"Obs: {obs.get('temperature_display')}°F", "ok")
    except Exception as e:
        errors.append(f"Obs: {e}")
        add_log(f"Obs error: {e}", "err")

    # Wethr high
    try:
        wh = wethr_get(f"observations.php?station_code={STATION}&mode=wethr_high&logic=nws")
        state["wethr_high"] = wh
        add_log(f"Wethr High: {wh.get('wethr_high')}°F", "ok")
    except Exception as e:
        errors.append(f"WethrHigh: {e}")
        add_log(f"Wethr High error: {e}", "err")

    # Forecasts per model
    for model in active_models():
        try:
            # NWS uses versioned forecasts with run=current; all others use run=latest
            run_param = "current" if model == "NWS" else "latest"
            data = wethr_get(f"forecasts.php?location_name={STATION}&model={requests.utils.quote(model)}&run={run_param}")
            if model == "NWS":
                add_log(f"NWS raw: type={type(data).__name__} len={len(data) if isinstance(data,(list,dict)) else '?'} sample={str(data)[:120]}", "info")
            # API returns either a list directly or a dict with a forecasts key
            if isinstance(data, list):
                temps = data
                meta = {}
            else:
                temps = data.get("forecasts", [])
                meta = data

            if temps:
                def get_temp(x):
                    for k in ["temperature_f","temperature_display","temperature","temp","value","high"]:
                        v = x.get(k)
                        if v is not None:
                            try: return round(float(v), 1)
                            except: pass
                    return None

                # OKC is CDT (UTC-5). Local day runs from 05:00 UTC to 04:59 UTC next day.
                from datetime import timedelta
                utc_now = datetime.utcnow()
                # Start of today in OKC = today at 05:00 UTC (midnight CDT)
                okc_local = utc_now - timedelta(hours=5)
                day_start_utc = okc_local.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(hours=5)
                day_end_utc = day_start_utc + timedelta(hours=24)
                def parse_vt(x):
                    vt = str(x.get("valid_time",""))
                    try: return datetime.strptime(vt[:16], "%Y-%m-%d %H:%M")
                    except: return None
                today_temps = [x for x in temps if parse_vt(x) is not None and day_start_utc <= parse_vt(x) < day_end_utc]
                if not today_temps:
                    today_temps = temps  # fallback

                # Max temp among today's entries = forecast high
                max_entry = max(today_temps, key=lambda x: get_temp(x) or 0)
                raw_temp = get_temp(max_entry)

                # Current-hour entry = temp model predicted for right now (for pacing)
                now_utc = datetime.utcnow()
                def time_diff(x):
                    vt = x.get("valid_time","")
                    try:
                        from datetime import datetime as dt
                        t = dt.strptime(vt[:16], "%Y-%m-%d %H:%M")
                        return abs((t - now_utc).total_seconds())
                    except:
                        return float("inf")
                current_entry = min(temps, key=time_diff)
                current_temp = get_temp(current_entry)
                # Get run time and format as e.g. "12Z"
                run_raw = (meta.get("run_time") or meta.get("run") or
                           max_entry.get("run_time") or max_entry.get("run") or "")
                try:
                    # Convert "2026-05-23 06:00:00" -> "06Z"
                    run_fmt = run_raw[11:13] + "Z" if len(run_raw) >= 13 else run_raw or "—"
                except:
                    run_fmt = run_raw or "—"
                state["forecasts"][model] = {
                    "high": raw_temp,
                    "current_fcst": current_temp,
                    "run": run_fmt,
                    "forecast_time": (meta.get("valid_time") or max_entry.get("valid_time") or
                                      max_entry.get("forecast_time") or max_entry.get("time") or "—"),
                }
                add_log(f"{model}: high={raw_temp}° now={current_temp}° run {run_fmt} (today_entries={len(today_temps)})", "ok")
        except Exception as e:
            err_str = str(e)
            errors.append(f"{model}: {err_str}")
            add_log(f"{model} error: {err_str[:80]}", "warn")

    add_log(f"Done. {len(state['forecasts'])} models loaded.", "ok")
    state["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    state["errors"] = errors

def background_loop():
    while True:
        try:
            t = threading.Thread(target=fetch_all, daemon=True)
            t.start()
            t.join(timeout=90)  # give up after 90 seconds
            if t.is_alive():
                add_log("Fetch timed out after 90s", "err")
        except Exception as e:
            add_log(f"Background loop error: {e}", "err")
        time.sleep(REFRESH_SEC)

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/api/state")
def api_state():
    acc = state["accuracy"]
    rows = []
    models = active_models()
    for i, model in enumerate(models):
        a = acc.get(model, {})
        fcst = state["forecasts"].get(model, {})
        raw = fcst.get("high")
        corr = a.get("correction")
        adj = round(raw + float(corr), 1) if raw is not None and corr not in (None, "") else None
        obs_temp = (state["obs"] or {}).get("temperature_display")
        current_fcst = fcst.get("current_fcst")
        pace = round(float(obs_temp) - float(current_fcst), 1) if obs_temp and current_fcst else None
        rows.append({
            "rank": i+1, "model": model,
            "run": fcst.get("run", "—"),
            "raw_high": raw,
            "correction": corr,
            "adj_high": adj,
            "pace": pace,
            "mae": a.get("mae"),
            "rmse": a.get("rmse"),
            "runs": a.get("runs", {}),
        })

    # MAE-weighted consensus
    consensus = None
    w_sum, w_total = 0, 0
    for r in rows:
        mae = r["mae"]
        adj = r["adj_high"] if r["adj_high"] is not None else r["raw_high"]
        if mae and adj and float(mae) > 0:
            w = 1 / float(mae)
            w_sum += adj * w
            w_total += w
    if w_total > 0:
        consensus = round(w_sum / w_total, 1)

    return jsonify({
        "obs": state["obs"],
        "wethr_high": state["wethr_high"],
        "rows": rows,
        "consensus": consensus,
        "last_updated": state["last_updated"],
        "errors": state["errors"],
        "log": state["log"][:30],
        "models": active_models(),
    })

@app.route("/api/accuracy", methods=["POST"])
def save_accuracy():
    data = request.json or {}
    state["accuracy"] = data
    add_log("Accuracy data updated", "ok")
    return jsonify({"ok": True})

@app.route("/api/refresh", methods=["POST"])
def manual_refresh():
    threading.Thread(target=fetch_all, daemon=True).start()
    return jsonify({"ok": True})

@app.route("/")
def index():
    return render_template_string(HTML)

# ── HTML ──────────────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>KOKC Model Tracker</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600;700&display=swap');
  *{box-sizing:border-box;margin:0;padding:0}
  :root{
    --bg:#080c10;--bg2:#0e1520;--bg3:#0b1118;
    --border:#1a2535;--text:#c9d4e0;--dim:#4a6080;--dimmer:#2a3a50;
    --blue:#38bdf8;--green:#4ade80;--yellow:#facc15;--red:#f87171;--purple:#c084fc;
    --orange:#fb923c;
  }
  body{background:var(--bg);color:var(--text);font-family:'IBM Plex Mono',monospace;font-size:13px;min-height:100vh}
  header{background:var(--bg3);border-bottom:1px solid var(--border);padding:14px 20px;position:sticky;top:0;z-index:20;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px}
  .header-left h1{font-size:18px;color:#e8f0f8;letter-spacing:-.5px}
  .header-left p{font-size:10px;color:var(--dim);letter-spacing:2px;text-transform:uppercase;margin-top:2px}
  .header-right{display:flex;align-items:center;gap:16px;flex-wrap:wrap}
  .stat-pill{text-align:right}
  .stat-pill .label{font-size:9px;color:var(--dim);letter-spacing:2px;text-transform:uppercase}
  .stat-pill .val{font-size:22px;font-weight:700;line-height:1.1}
  .stat-pill .sub{font-size:9px;color:var(--dimmer)}
  .sep{width:1px;height:40px;background:var(--border)}
  nav{display:flex;gap:2px;background:var(--bg3);border-bottom:1px solid var(--border);padding:0 20px}
  nav button{background:none;border:none;border-bottom:2px solid transparent;color:var(--dim);padding:10px 16px;font-size:11px;letter-spacing:1.5px;text-transform:uppercase;cursor:pointer;font-family:inherit;transition:color .15s}
  nav button.active{border-bottom-color:var(--blue);color:var(--blue)}
  main{padding:20px;max-width:1100px;margin:0 auto}
  .tab{display:none}.tab.active{display:block}
  .card{background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:16px 18px;margin-bottom:16px}
  .card-title{font-size:10px;letter-spacing:2.5px;color:var(--blue);text-transform:uppercase;margin-bottom:12px;display:flex;align-items:center;gap:8px}
  .stats-row{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:16px}
  .stat-card{background:#0b1520;border:1px solid var(--border);border-radius:6px;padding:12px 16px;flex:1;min-width:120px}
  .stat-card .lbl{font-size:9px;letter-spacing:2px;color:var(--dim);text-transform:uppercase}
  .stat-card .v{font-size:22px;font-weight:700;margin-top:4px;line-height:1}
  .stat-card .s{font-size:10px;color:var(--dimmer);margin-top:3px}
  table{width:100%;border-collapse:collapse}
  th{padding:7px 10px;text-align:left;font-size:10px;letter-spacing:1.5px;color:var(--dim);text-transform:uppercase;border-bottom:1px solid var(--border);white-space:nowrap}
  td{padding:8px 10px;border-bottom:1px solid #111922;white-space:nowrap}
  tr:nth-child(even) td{background:#0a1018}
  input[type=number]{background:var(--bg);border:1px solid #1e2e42;border-radius:4px;color:var(--text);padding:4px 8px;font-size:12px;width:70px;font-family:inherit;outline:none}
  input[type=number]:focus{border-color:var(--blue)}
  .btn{background:none;border:1px solid var(--blue);color:var(--blue);border-radius:4px;padding:6px 14px;font-size:11px;letter-spacing:1px;cursor:pointer;text-transform:uppercase;font-family:inherit}
  .btn:hover{background:#38bdf811}
  .btn-red{border-color:var(--red);color:var(--red)}
  .btn-red:hover{background:#f8717111}
  .btn-green{border-color:var(--green);color:var(--green)}
  .pill{border-radius:3px;padding:2px 7px;font-size:10px;font-weight:600}
  .pill-green{background:#4ade8022;color:var(--green)}
  .pill-yellow{background:#facc1522;color:var(--yellow)}
  .dot{width:7px;height:7px;border-radius:50%;display:inline-block;margin-right:5px}
  .dot-green{background:var(--green);box-shadow:0 0 6px var(--green)}
  .dot-red{background:var(--red);box-shadow:0 0 6px var(--red)}
  .dot-yellow{background:var(--yellow);box-shadow:0 0 6px var(--yellow)}
  .pace-bar-wrap{display:flex;flex-direction:column;gap:7px}
  .pace-row{display:flex;align-items:center;gap:10px}
  .pace-label{width:80px;font-size:11px;color:#8aabcc}
  .pace-track{width:180px;position:relative;height:12px}
  .pace-bar{height:10px;border-radius:3px;margin-top:1px}
  .log-box{background:#060a0e;border-radius:4px;padding:12px;max-height:400px;overflow-y:auto}
  .log-entry{margin-bottom:5px}
  .log-t{color:var(--dimmer)}
  .log-ok{color:var(--green)}
  .log-err{color:var(--red)}
  .log-warn{color:var(--yellow)}
  .log-info{color:var(--dim)}
  .status-bar{display:flex;align-items:center;gap:8px;font-size:10px;color:var(--dim)}
  .run-cards{display:flex;flex-wrap:wrap;gap:8px;margin-top:10px}
  .run-card{background:#0b1520;border:1px solid var(--border);border-radius:5px;padding:8px 12px;min-width:130px}
  .run-card .m{font-size:11px;color:#8aabcc;font-weight:600}
  .run-card .r{font-size:13px;color:var(--blue);margin-top:2px}
  .run-card .c{font-size:11px;margin-top:2px}
  @media(max-width:600px){.header-right{gap:8px}.stat-pill .val{font-size:17px}}
</style>
</head>
<body>

<header>
  <div class="header-left">
    <h1>KOKC · Model Tracker</h1>
    <p>Oklahoma City Will Rogers World Airport</p>
  </div>
  <div class="header-right">
    <div class="stat-pill">
      <div class="label">Live Obs</div>
      <div class="val" id="h-obs" style="color:var(--yellow)">—</div>
      <div class="sub" id="h-obs-time">awaiting</div>
    </div>
    <div class="sep"></div>
    <div class="stat-pill">
      <div class="label">Wethr High</div>
      <div class="val" id="h-wh" style="color:var(--green)">—</div>
      <div class="sub">NWS logic</div>
    </div>
    <div class="sep"></div>
    <div class="stat-pill">
      <div class="label">Consensus</div>
      <div class="val" id="h-con" style="color:var(--blue)">—</div>
      <div class="sub">MAE-weighted</div>
    </div>
    <div class="sep"></div>
    <div style="text-align:right">
      <div class="status-bar" id="status-bar">
        <span class="dot dot-yellow" id="status-dot"></span>
        <span id="status-text">Loading…</span>
      </div>
      <div style="font-size:10px;color:var(--dimmer);margin-top:3px">Next: <span id="countdown">5:00</span></div>
      <button class="btn" style="margin-top:4px;padding:3px 10px;font-size:10px" onclick="manualRefresh()">↺ Now</button>
    </div>
  </div>
</header>

<nav>
  <button class="active" onclick="showTab('dashboard',this)">📊 Dashboard</button>
  <button onclick="showTab('entry',this)">☀️ Morning Entry</button>
  <button onclick="showTab('runs',this)">🕐 Run Accuracy</button>
  <button onclick="showTab('log',this)">📟 Log</button>
</nav>

<main>

<!-- DASHBOARD -->
<div class="tab active" id="tab-dashboard">
  <div class="stats-row">
    <div class="stat-card"><div class="lbl">Current Temp</div><div class="v" id="s-obs" style="color:var(--yellow)">—</div><div class="s" id="s-obs-t">awaiting</div></div>
    <div class="stat-card"><div class="lbl">Wethr High</div><div class="v" id="s-wh" style="color:var(--green)">—</div><div class="s">NWS · trading day</div></div>
    <div class="stat-card"><div class="lbl">Consensus High</div><div class="v" id="s-con" style="color:var(--blue)">—</div><div class="s">MAE-weighted adj</div></div>
    <div class="stat-card"><div class="lbl">Models Live</div><div class="v" id="s-mods" style="color:var(--purple)">—</div><div class="s">forecast runs</div></div>
  </div>

  <div class="card">
    <div class="card-title">
      Top 10 Models — Live Forecasts + Accuracy Adjustments
      <span class="pill pill-yellow" id="acc-badge" style="display:none">→ Enter accuracy in Morning Entry</span>
      <span class="pill pill-green" id="acc-loaded" style="display:none">Accuracy loaded ✓</span>
    </div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr>
          <th>#</th><th>Model</th><th>Run</th><th>Fcst High</th>
          <th>Correction</th><th>Adj High</th><th>Obs Pace</th><th>MAE</th><th>RMSE</th>
        </tr></thead>
        <tbody id="main-table"></tbody>
      </table>
    </div>
  </div>

  <div class="card" id="pace-card" style="display:none">
    <div class="card-title">Model Pacing vs Current Obs (<span id="pace-obs-val">—</span>°F)</div>
    <div class="pace-bar-wrap" id="pace-bars"></div>
    <div style="font-size:10px;color:var(--dimmer);margin-top:10px">
      Pace = current obs minus model forecast high. Green = running warmer than model.
    </div>
  </div>
</div>

<!-- MORNING ENTRY -->
<div class="tab" id="tab-entry">

  <!-- FAST PATH: JSON paste -->
  <div class="card" style="border-color:#1e3a5f">
    <div class="card-title">⚡ Fast Import — Paste JSON from Claude</div>
    <p style="color:var(--dim);font-size:12px;line-height:1.7;margin-bottom:12px">
      Each morning: screenshot the accuracy tables → send to Claude → paste the JSON it gives you here → done in seconds.
    </p>
    <textarea id="json-paste" placeholder='Paste JSON here, e.g. {"ARPEGE":{"mae":0.7,"correction":0.3,"rmse":0.8,"runs":{"00Z":{"mae":0.9,"correction":0.2}}},...}'
      style="width:100%;height:110px;background:#060a0e;border:1px solid #1e3a5f;border-radius:4px;color:#c9d4e0;padding:10px;font-family:inherit;font-size:11px;resize:vertical;outline:none"></textarea>
    <div style="display:flex;gap:10px;align-items:center;margin-top:10px;flex-wrap:wrap">
      <button class="btn" style="border-color:#38bdf8;color:#38bdf8" onclick="loadFromJSON()">⚡ Load JSON</button>
      <span style="font-size:10px;color:var(--dim)" id="json-status"></span>
    </div>
  </div>

  <!-- MANUAL FALLBACK -->
  <details style="margin-bottom:16px">
    <summary style="cursor:pointer;color:var(--dim);font-size:11px;letter-spacing:1px;padding:10px 0;list-style:none">
      ▸ Manual entry (fallback if no Claude available)
    </summary>
    <div style="margin-top:12px">
      <div class="card">
        <div class="card-title">Overall 7D Accuracy</div>
        <div style="overflow-x:auto">
          <table>
            <thead><tr><th>Model</th><th>MAE (°F)</th><th>Correction (°F)</th><th>RMSE (°F)</th></tr></thead>
            <tbody id="acc-overall-table"></tbody>
          </table>
        </div>
      </div>
      <div class="card">
        <div class="card-title">Run-Specific Corrections</div>
        <p style="color:var(--dim);font-size:11px;margin-bottom:12px">Leave blank if a model doesn't run that cycle.</p>
        <div style="overflow-x:auto">
          <table id="run-entry-table">
            <thead>
              <tr><th>Model</th><th>00Z</th><th>03Z</th><th>06Z</th><th>09Z</th><th>12Z</th><th>15Z</th><th>18Z</th><th>21Z</th></tr>
              <tr><th style="font-size:9px;color:var(--dimmer)">MAE / Corr</th>
                <th colspan="8" style="font-size:9px;color:var(--dimmer);text-align:left;padding-left:10px">MAE on top · Correction below</th></tr>
            </thead>
            <tbody id="run-entry-body"></tbody>
          </table>
        </div>
        <div style="margin-top:14px;display:flex;gap:10px;align-items:center;flex-wrap:wrap">
          <button class="btn btn-green" onclick="saveAccuracy()">💾 Save</button>
          <button class="btn btn-red" onclick="clearAccuracy()">Clear All</button>
          <span style="font-size:10px;color:var(--dim)" id="save-status"></span>
        </div>
      </div>
    </div>
  </details>

  <!-- Current loaded data preview -->
  <div class="card" id="acc-preview" style="display:none">
    <div class="card-title">Currently Loaded Accuracy Data</div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr><th>Model</th><th>MAE</th><th>Correction</th><th>RMSE</th><th>Runs with data</th></tr></thead>
        <tbody id="acc-preview-body"></tbody>
      </table>
    </div>
    <div style="margin-top:10px;display:flex;gap:10px;align-items:center">
      <button class="btn btn-red" onclick="clearAccuracy()">Clear All</button>
      <span style="font-size:10px;color:var(--dim)" id="acc-loaded-time"></span>
    </div>
  </div>

</div>

<!-- RUN ACCURACY -->
<div class="tab" id="tab-runs">
  <div class="card">
    <div class="card-title">Run-Specific Accuracy — Entered Values</div>
    <div style="overflow-x:auto"><table>
      <thead><tr><th>Model</th><th>00Z</th><th>03Z</th><th>06Z</th><th>09Z</th><th>12Z</th><th>15Z</th><th>18Z</th><th>21Z</th></tr></thead>
      <tbody id="run-view-body"></tbody>
    </table></div>
    <div class="card-title" style="margin-top:20px">Current Live Run per Model</div>
    <div class="run-cards" id="run-cards"></div>
  </div>
</div>

<!-- LOG -->
<div class="tab" id="tab-log">
  <div class="card">
    <div class="card-title">Fetch Log</div>
    <div class="log-box" id="log-box"><div style="color:var(--dimmer)">No entries yet.</div></div>
  </div>
</div>

</main>

<script>
const ALL_KNOWN_MODELS = ["ARPEGE","HRRR","UKMO","LAV-MOS","NAM","RAP","GEM-GDPS","NWS","NAM-MOS","NBM",
  "NAM4KM","GFS","ICON","GFS-MOS","ECMWF-HRES","GEFS","JMA","RDPS","SREF","NBM-ENS"];
let MODELS = ALL_KNOWN_MODELS.slice(0,10); // updated dynamically from server
const RUNS = ["00Z","03Z","06Z","09Z","12Z","15Z","18Z","21Z"];
let accData = JSON.parse(localStorage.getItem("kokc_acc") || "{}");
// Restore model order from last saved accuracy data
if(Object.keys(accData).length) MODELS = Object.keys(accData);
let countdown = 300;
let countdownTimer;

// ── Helpers ──────────────────────────────────────────────────────────────────
function fmt1(v){ return (v==null||v==="") ? "—" : Number(v).toFixed(1); }
function fmtCorr(v){
  if(v==null||v==="") return "—";
  const n=Number(v); return (n>=0?"+":"")+n.toFixed(1)+"°";
}
function corrColor(v){
  if(v==null||v==="") return "#4a6080";
  return Number(v)>0?"#60a5fa":Number(v)<0?"#f87171":"#4a6080";
}
function maeColor(v){
  if(v==null||v==="") return "#4a6080";
  const n=Number(v); return n<=1?"#4ade80":n<=2?"#facc15":"#f87171";
}
function paceColor(v){
  const n=Math.abs(Number(v)); return n<=1?"#4ade80":n<=3?"#facc15":"#f87171";
}

// ── Tab switching ─────────────────────────────────────────────────────────────
function showTab(id, btn){
  document.querySelectorAll(".tab").forEach(t=>t.classList.remove("active"));
  document.querySelectorAll("nav button").forEach(b=>b.classList.remove("active"));
  document.getElementById("tab-"+id).classList.add("active");
  btn.classList.add("active");
}

// ── Build entry forms ─────────────────────────────────────────────────────────
function buildEntryForms(){
  // Overall
  const tbody = document.getElementById("acc-overall-table");
  tbody.innerHTML = MODELS.map((m,i)=>`
    <tr style="${i%2?"background:#0a1018":""}">
      <td style="color:#e8f0f8;font-weight:600">${m}</td>
      <td><input type="number" step="0.1" placeholder="0.0" id="ov-mae-${m}" value="${accData[m]?.mae||""}"></td>
      <td><input type="number" step="0.1" placeholder="0.0" id="ov-corr-${m}" value="${accData[m]?.correction||""}"></td>
      <td><input type="number" step="0.1" placeholder="0.0" id="ov-rmse-${m}" value="${accData[m]?.rmse||""}"></td>
    </tr>`).join("");

  // Run-specific
  const rbody = document.getElementById("run-entry-body");
  rbody.innerHTML = MODELS.map((m,i)=>`
    <tr style="${i%2?"background:#0a1018":""}">
      <td style="color:#8aabcc;font-weight:600">${m}</td>
      ${RUNS.map(r=>`
        <td style="padding:5px 6px">
          <div style="display:flex;flex-direction:column;gap:3px">
            <input type="number" step="0.1" placeholder="MAE" style="width:56px;font-size:11px"
              id="run-mae-${m}-${r}" value="${accData[m]?.runs?.[r]?.mae||""}">
            <input type="number" step="0.1" placeholder="Corr" style="width:56px;font-size:11px"
              id="run-corr-${m}-${r}" value="${accData[m]?.runs?.[r]?.correction||""}">
          </div>
        </td>`).join("")}
    </tr>`).join("");
}

function loadFromJSON(){
  const raw = document.getElementById("json-paste").value.trim();
  const status = document.getElementById("json-status");
  if(!raw){ status.style.color="var(--red)"; status.textContent="Nothing to paste."; return; }
  try {
    const parsed = JSON.parse(raw);
    // Validate it has at least one known model
    const known = MODELS.filter(m => parsed[m]);
    if(known.length === 0){ status.style.color="var(--red)"; status.textContent="No recognisable models found in JSON."; return; }
    accData = parsed;
    MODELS = Object.keys(parsed); // update model list immediately
    localStorage.setItem("kokc_acc", JSON.stringify(parsed));
    localStorage.setItem("kokc_acc_time", new Date().toLocaleString());
    fetch("/api/accuracy",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(parsed)})
      .then(()=>{
        status.style.color="var(--green)";
        status.textContent = `✓ Loaded ${known.length} models at ${new Date().toLocaleTimeString()}`;
        document.getElementById("json-paste").value = "";
        buildEntryForms();
        renderAccPreview();
        poll();
      })
      .catch(()=>{ status.style.color="var(--red)"; status.textContent="Server save failed."; });
  } catch(e) {
    status.style.color="var(--red)";
    status.textContent = "Invalid JSON: " + e.message;
  }
}

function renderAccPreview(){
  const hasAny = MODELS.some(m => accData[m]?.mae);
  const preview = document.getElementById("acc-preview");
  if(!hasAny){ preview.style.display="none"; return; }
  preview.style.display="block";
  const t = localStorage.getItem("kokc_acc_time");
  if(t) document.getElementById("acc-loaded-time").textContent = "Loaded: "+t;
  document.getElementById("acc-preview-body").innerHTML = MODELS.map((m,i)=>{
    const a = accData[m]||{};
    const runsWithData = Object.entries(a.runs||{}).filter(([,v])=>v.mae||v.correction).map(([k])=>k).join(", ")||"—";
    return `<tr style="${i%2?"background:#0a1018":""}">
      <td style="color:#e8f0f8;font-weight:600">${m}</td>
      <td style="color:${maeColor(a.mae)}">${a.mae?fmt1(a.mae)+"°":"—"}</td>
      <td style="color:${corrColor(a.correction)}">${a.correction!=null&&a.correction!==""?fmtCorr(a.correction):"—"}</td>
      <td style="color:#4a6080">${a.rmse?fmt1(a.rmse)+"°":"—"}</td>
      <td style="color:#4a6080;font-size:11px">${runsWithData}</td>
    </tr>`;
  }).join("");
}

function saveAccuracy(){
  const data = {};
  MODELS.forEach(m=>{
    data[m] = {
      mae: document.getElementById(`ov-mae-${m}`)?.value || "",
      correction: document.getElementById(`ov-corr-${m}`)?.value || "",
      rmse: document.getElementById(`ov-rmse-${m}`)?.value || "",
      runs: {}
    };
    RUNS.forEach(r=>{
      data[m].runs[r] = {
        mae: document.getElementById(`run-mae-${m}-${r}`)?.value || "",
        correction: document.getElementById(`run-corr-${m}-${r}`)?.value || ""
      };
    });
  });
  accData = data;
  localStorage.setItem("kokc_acc", JSON.stringify(data));
  fetch("/api/accuracy",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(data)})
    .then(()=>{ document.getElementById("save-status").textContent = "✓ Saved at "+new Date().toLocaleTimeString(); })
    .catch(()=>{ document.getElementById("save-status").textContent = "⚠ Save failed"; });
}

function clearAccuracy(){
  if(!confirm("Clear all accuracy data?")) return;
  accData = {};
  localStorage.removeItem("kokc_acc");
  localStorage.removeItem("kokc_acc_time");
  fetch("/api/accuracy",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({})});
  buildEntryForms();
  renderAccPreview();
  document.getElementById("save-status").textContent = "Cleared";
}

// ── Render state ──────────────────────────────────────────────────────────────
function render(data){
  if(data.models && data.models.length) MODELS = data.models;
  const obs = data.obs;
  const wh = data.wethr_high;
  const rows = data.rows;
  const con = data.consensus;

  // Header
  if(obs){
    const temp = obs.temperature_display;
    document.getElementById("h-obs").textContent = temp+"°F";
    document.getElementById("s-obs").textContent = temp+"°";
    document.getElementById("h-obs-time").textContent = (obs.observation_time||"").slice(11,16)||"—";
    document.getElementById("s-obs-t").textContent = (obs.observation_time||"").slice(11,16)||"—";
  }
  if(wh){
    document.getElementById("h-wh").textContent = wh.wethr_high+"°F";
    document.getElementById("s-wh").textContent = wh.wethr_high+"°";
  }
  if(con){
    document.getElementById("h-con").textContent = con+"°F";
    document.getElementById("s-con").textContent = con+"°";
  }

  // Models live count
  const liveCt = rows.filter(r=>r.raw_high!=null).length;
  document.getElementById("s-mods").textContent = liveCt+"/"+MODELS.length;

  // Accuracy badge
  const hasAcc = rows.some(r=>r.mae!=null&&r.mae!=="");
  document.getElementById("acc-badge").style.display = hasAcc?"none":"inline";
  document.getElementById("acc-loaded").style.display = hasAcc?"inline":"none";

  // Main table
  document.getElementById("main-table").innerHTML = rows.map(r=>`
    <tr>
      <td style="color:#4a6080">#${r.rank}</td>
      <td style="color:#e8f0f8;font-weight:600">${r.model}</td>
      <td style="color:#4a6080;font-size:11px">${r.run||"—"}</td>
      <td style="color:var(--yellow)">${r.raw_high!=null?r.raw_high+"°":"—"}</td>
      <td style="color:${corrColor(r.correction)}">${fmtCorr(r.correction)}</td>
      <td style="color:var(--green);font-weight:600">${r.adj_high!=null?r.adj_high+"°":"—"}</td>
      <td style="color:${r.pace!=null?paceColor(r.pace):"#1e2e42"}">${r.pace!=null?(r.pace>=0?"+":"")+r.pace+"°":"—"}</td>
      <td style="color:${maeColor(r.mae)}">${r.mae&&r.mae!=""?fmt1(r.mae)+"°":"—"}</td>
      <td style="color:#4a6080">${r.rmse&&r.rmse!=""?fmt1(r.rmse)+"°":"—"}</td>
    </tr>`).join("");

  // Pacing bars
  const paceRows = rows.filter(r=>r.pace!=null);
  if(paceRows.length && obs){
    document.getElementById("pace-card").style.display="block";
    document.getElementById("pace-obs-val").textContent = obs.temperature_display;
    document.getElementById("pace-bars").innerHTML = paceRows.map(r=>{
      const p = Number(r.pace);
      const w = Math.min(Math.abs(p)*14,140);
      const col = p>=0?"#4ade80":"#f87171";
      return `<div class="pace-row">
        <div class="pace-label">${r.model}</div>
        <div class="pace-track">
          <div class="pace-bar" style="width:${w}px;background:${col}33;border:1px solid ${col}"></div>
        </div>
        <span style="font-size:11px;color:${paceColor(r.pace)};font-weight:600">${p>=0?"+":""}${r.pace}°</span>
      </div>`;
    }).join("");
  }

  // Run view tab
  document.getElementById("run-view-body").innerHTML = rows.map((r,i)=>`
    <tr style="${i%2?"background:#0a1018":""}">
      <td style="color:#e8f0f8;font-weight:600">${r.model}</td>
      ${RUNS.map(run=>{
        const rd = r.runs?.[run]||{};
        const hasMae = rd.mae!=null&&rd.mae!=="";
        const hasCorr = rd.correction!=null&&rd.correction!=="";
        return `<td style="text-align:center">${(hasMae||hasCorr)?`
          <div style="line-height:1.8">
            ${hasMae?`<div style="color:${maeColor(rd.mae)}">${fmt1(rd.mae)}°</div>`:""}
            ${hasCorr?`<div style="color:${corrColor(rd.correction)}">${fmtCorr(rd.correction)}</div>`:""}
          </div>`:`<span style="color:#1e2e42">—</span>`}</td>`;
      }).join("")}
    </tr>`).join("");

  // Run cards
  document.getElementById("run-cards").innerHTML = rows.map(r=>{
    const runKey = (r.run||"").replace(/[^0-9]/g,"").slice(0,2)+"Z";
    const rd = r.runs?.[runKey]||{};
    const hasCorr = rd.correction!=null&&rd.correction!=="";
    return `<div class="run-card">
      <div class="m">${r.model}</div>
      <div class="r">${r.run||"—"}</div>
      ${hasCorr?`<div class="c" style="color:${corrColor(rd.correction)}">Corr: ${fmtCorr(rd.correction)}</div>`
               :`<div class="c" style="color:#2a4060">No run corr</div>`}
    </div>`;
  }).join("");

  // Log
  if(data.log&&data.log.length){
    document.getElementById("log-box").innerHTML = data.log.map(e=>
      `<div class="log-entry"><span class="log-t">[${e.t}]</span> <span class="log-${e.level}">${e.msg}</span></div>`
    ).join("");
  }

  // Status
  const dot = document.getElementById("status-dot");
  const txt = document.getElementById("status-text");
  dot.className = "dot " + (data.errors?.length ? "dot-yellow" : "dot-green");
  txt.textContent = data.last_updated ? "Updated "+data.last_updated.slice(11,16) : "Live";
}

// ── Polling ───────────────────────────────────────────────────────────────────
function poll(){
  // Push saved accuracy to server on load
  if(Object.keys(accData).length){
    fetch("/api/accuracy",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(accData)});
  }
  fetch("/api/state").then(r=>r.json()).then(render).catch(console.error);
}

function manualRefresh(){
  fetch("/api/refresh",{method:"POST"});
  countdown = 300;
  setTimeout(poll, 3000);
}

function startCountdown(){
  clearInterval(countdownTimer);
  countdown = 300;
  countdownTimer = setInterval(()=>{
    countdown = Math.max(0, countdown-1);
    const m = Math.floor(countdown/60);
    const s = String(countdown%60).padStart(2,"0");
    document.getElementById("countdown").textContent = m+":"+s;
    if(countdown === 0){ poll(); countdown = 300; }
  }, 1000);
}

// ── Init ──────────────────────────────────────────────────────────────────────
buildEntryForms();
renderAccPreview();
poll();
startCountdown();
setInterval(poll, 300000);
</script>
</body>
</html>
"""

if os.path.exists("accuracy.json"):
    try:
        with open("accuracy.json") as f:
            state["accuracy"] = json.load(f)
    except Exception:
        pass

_started = False
_start_lock = threading.Lock()

def start_background():
    global _started
    with _start_lock:
        if not _started:
            _started = True
            t = threading.Thread(target=background_loop, daemon=True)
            t.start()

# Works with both gunicorn and direct run
with app.app_context():
    start_background()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)














