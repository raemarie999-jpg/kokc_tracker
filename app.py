import os, json, time, threading
from collections import deque
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, render_template_string
import requests
import pytz

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
STATIONS = ["KOKC", "KPHL"]
STATION_NAMES = {"KOKC": "Oklahoma City", "KPHL": "Philadelphia"}

ALL_KNOWN_MODELS = [
    "ARPEGE","HRRR","UKMO","LAV-MOS","NAM","RAP","GEM-GDPS","NAM-MOS","NBM",
    "NAM4KM","GFS","ICON","GFS-MOS","ECMWF-HRES","GEFS","JMA","RDPS","SREF"
]
RUN_CYCLES = ["00Z","01Z","02Z","03Z","04Z","05Z","06Z","07Z","08Z","09Z","10Z","11Z",
              "12Z","13Z","14Z","15Z","16Z","17Z","18Z","19Z","20Z","21Z","22Z","23Z"]
REFRESH_SEC = 600

# Shared session for connection pooling across all model fetches
_session = requests.Session()

# In-memory caches to avoid repeated disk reads on every API call
_history_cache = {}   # station -> {"data": {}, "ts": float}
_pacing_cache = {}    # station -> {"data": {}, "ts": float}
_CACHE_TTL = 60       # seconds before re-reading from disk

def get_history(station):
    c = _history_cache.get(station)
    if c and (time.time() - c["ts"]) < _CACHE_TTL:
        return c["data"]
    data = load_json_file(f"{DATA_DIR}/history_{station}.json", {})
    _history_cache[station] = {"data": data, "ts": time.time()}
    return data

def invalidate_history_cache(station):
    _history_cache.pop(station, None)

def get_pacing(station):
    c = _pacing_cache.get(station)
    if c and (time.time() - c["ts"]) < _CACHE_TTL:
        return c["data"]
    data = load_json_file(f"{DATA_DIR}/pacing_{station}.json", {})
    _pacing_cache[station] = {"data": data, "ts": time.time()}
    return data

def invalidate_pacing_cache(station):
    _pacing_cache.pop(station, None)

def make_state():
    return {
        "obs": None,
        "wethr_high": None,
        "forecasts": {},
        "nws_versions": {},
        "accuracy": {},
        "last_updated": None,
        "errors": [],
        "log": deque(maxlen=100),
        "today_avg_pace": {},
        "consensus_snapshots": [],
    }

states = {s: make_state() for s in STATIONS}

def get_state(station=None):
    return states.get(station or "KOKC", states["KOKC"])

def active_models(station="KOKC"):
    acc = get_state(station).get("accuracy", {})
    models = [m for m in acc.keys() if m != "NWS"] if acc else ALL_KNOWN_MODELS
    return models

def add_log(msg, level="info", station="KOKC"):
    entry = {"t": datetime.now().strftime("%H:%M:%S"), "msg": msg, "level": level}
    st = get_state(station)
    st["log"].appendleft(entry)
    print(f"[{station}][{entry['t']}] {msg}")

def wethr_get(path):
    r = _session.get(
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

def okc_day_bounds(offset=0):
    now_local = datetime.now(pytz.utc).astimezone(OKC_TZ).replace(tzinfo=None)
    day_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=offset)
    day_start_utc = OKC_TZ.localize(day_start_local).astimezone(pytz.utc).replace(tzinfo=None)
    day_end_utc = day_start_utc + timedelta(hours=24)
    return day_start_utc, day_end_utc

def today_entries(temps):
    day_start, day_end = okc_day_bounds(0)
    filtered = [x for x in temps if parse_vt(x) is not None and day_start <= parse_vt(x) < day_end]
    return filtered if filtered else temps

def tomorrow_entries(temps):
    day_start, day_end = okc_day_bounds(1)
    filtered = [x for x in temps if parse_vt(x) is not None and day_start <= parse_vt(x) < day_end]
    return filtered

def fmt_run(run_raw):
    try:
        if len(run_raw) >= 13:
            return run_raw[11:13] + "Z"
        return run_raw or "—"
    except:
        return "—"

def fetch_all(station="KOKC"):
    st = get_state(station)
    if not API_KEY:
        add_log("No API key set", "err", station)
        return
    add_log("Fetching data...", "info", station)
    errors = []

    # Observation
    try:
        obs = wethr_get(f"observations.php?station_code={station}&mode=latest")
        st["obs"] = obs
        add_log(f"Obs: {obs.get('temperature_display')}F", "ok", station)
    except Exception as e:
        errors.append(f"Obs: {e}")
        add_log(f"Obs error: {e}", "err", station)

    # Wethr high
    try:
        wh = wethr_get(f"observations.php?station_code={station}&mode=wethr_high&logic=nws")
        st["wethr_high"] = wh
        add_log(f"Wethr High: {wh.get('wethr_high')}F", "ok", station)
    except Exception as e:
        errors.append(f"WethrHigh: {e}")
        add_log(f"Wethr High error: {e}", "err", station)

    # Forecasts per model — only fetch models in accuracy data, skip if none loaded
    fetch_targets = active_models(station)
    if not fetch_targets:
        add_log("No accuracy data yet — skipping model fetch", "warn", station)
        return
    utc_now = datetime.utcnow()
    for model in fetch_targets:
        try:
            data = wethr_get(f"forecasts.php?location_name={station}&model={requests.utils.quote(model)}&run=latest")
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
                # Tomorrow's high and low
                tomorrows = tomorrow_entries(temps)
                tmr_max = max(tomorrows, key=lambda x: get_temp(x) or 0) if tomorrows else None
                tmr_temp = get_temp(tmr_max) if tmr_max else None
                tmr_min = min(tomorrows, key=lambda x: get_temp(x) or 999) if tomorrows else None
                tmr_low = get_temp(tmr_min) if tmr_min else None
                tmr_low_time = None
                if tmr_min:
                    vt = parse_vt(tmr_min)
                    if vt:
                        local_vt = vt - timedelta(hours=5)
                        tmr_low_time = local_vt.strftime("%-I:%M%p").lower()

                st["forecasts"][model] = {
                    "high": raw_temp,
                    "current_fcst": current_temp,
                    "run": run_fmt,
                    "tmr_high": tmr_temp,
                    "tmr_low": tmr_low,
                    "tmr_low_time": tmr_low_time,
                }
                add_log(f"{model}: high={raw_temp} now={current_temp} run={run_fmt} ({len(todays)} entries)", "ok", station)
        except Exception as e:
            errors.append(f"{model}: {e}")
            add_log(f"{model} error: {str(e)[:80]}", "warn", station)

    # NWS skipped for now — endpoint TBD
    st["nws_versions"] = {}

    st["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st["errors"] = errors
    add_log(f"Done. {len(st['forecasts'])} models loaded.", "ok", station)

    # Save pacing snapshot here — guaranteed to run after fetch completes
    try:
        rows = build_snapshot_rows(station)
        save_pacing_snapshot(rows, station)
    except Exception as e:
        add_log(f"Snapshot error: {e}", "warn", station)
    # Save consensus snapshot every 30 min
    try:
        now_local = okc_local_now()
        if now_local.minute < 10 or (now_local.minute >= 30 and now_local.minute < 40):
            save_consensus_snapshot(station)
    except Exception as e:
        add_log(f"Consensus snapshot error: {e}", "warn", station)


OKC_TZ = pytz.timezone("America/Chicago")

def okc_local_now():
    return datetime.now(pytz.utc).astimezone(OKC_TZ).replace(tzinfo=None)

# In-memory snapshot store: date_str -> {model: {"sum": float, "count": int}}
_memory_snapshot_sums = {}
# Also keep raw list for disk persistence (bounded to today only)
_memory_snapshots = {}

def save_pacing_snapshot(rows, station="KOKC"):
    st = get_state(station)
    now = okc_local_now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M")

    # Build entry
    entry = {"time": time_str}
    for r in rows:
        if r.get("pace") is not None:
            entry[r["model"]] = r["pace"]

    # Store raw entry for disk persistence
    if date_str not in _memory_snapshots:
        _memory_snapshots[date_str] = []
    _memory_snapshots[date_str].append(entry)

    # Update pre-computed sums (O(1) per model instead of O(N) rescan)
    if date_str not in _memory_snapshot_sums:
        _memory_snapshot_sums[date_str] = {}
    sums = _memory_snapshot_sums[date_str]
    for r in rows:
        m = r["model"]
        p = r.get("pace")
        if p is not None:
            if m not in sums:
                sums[m] = {"sum": 0.0, "count": 0}
            sums[m]["sum"] += p
            sums[m]["count"] += 1

    # Compute rolling average from pre-computed sums (O(M) not O(N*M))
    avg = {}
    for m, s in sums.items():
        if s["count"] > 0:
            avg[m] = round(s["sum"] / s["count"], 2)
    st["today_avg_pace"] = avg

    # Best-effort disk save for persistence across deploys
    try:
        ensure_data_dir()
        disk = get_pacing(station)
        if date_str not in disk:
            disk[date_str] = []
        disk[date_str].append(entry)
        keys = sorted(disk.keys())
        if len(keys) > 60:
            for k in keys[:-60]:
                del disk[k]
        save_json_file(f"{DATA_DIR}/pacing_{station}.json", disk)
        invalidate_pacing_cache(station)
    except Exception as e:
        add_log(f"Disk snapshot error (non-fatal): {e}", "warn", station)

    add_log(f"Snapshot: {len([r for r in rows if r.get('pace') is not None])} models | avg pace sample: {list(avg.items())[:3]}", "info", station)

def rollup_daily_history(station="KOKC"):
    now = okc_local_now()
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    snapshots = get_pacing(station)
    if yesterday not in snapshots or not snapshots[yesterday]:
        return
    history = get_history(station)
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
    save_json_file(f"{DATA_DIR}/history_{station}.json", history)
    invalidate_history_cache(station)
    add_log(f"Rolled up history for {yesterday} ({len(day_snaps)} snapshots)", "ok", station)

def build_snapshot_rows(station="KOKC"):
    st = get_state(station)
    acc = st["accuracy"]
    models = [m for m in acc.keys() if m != "NWS"] if acc else ALL_KNOWN_MODELS
    obs_temp = (st["obs"] or {}).get("temperature_display")
    rows = []
    for model in models:
        fcst = st["forecasts"].get(model, {})
        current_fcst = fcst.get("current_fcst")
        try:
            pace = round(float(obs_temp) - float(current_fcst), 2) if obs_temp and current_fcst else None
        except:
            pace = None
        rows.append({"model": model, "pace": pace})
    return rows

def scheduled_fetch():
    for i, station in enumerate(STATIONS):
        if i > 0:
            time.sleep(30)
        t = threading.Thread(target=fetch_all, args=(station,), daemon=True)
        t.start()
        t.join(timeout=120)
        if t.is_alive():
            add_log("Fetch timed out", "err", station)

def save_consensus_snapshot(station="KOKC"):
    st = get_state(station)
    now = okc_local_now()
    # Only save between 6AM and 10PM local
    if now.hour < 6 or now.hour >= 22:
        return
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M")
    # Get current consensus and implied from state
    acc = st.get("accuracy", {})
    forecasts = st.get("forecasts", {})
    models = [m for m in acc.keys() if m != "NWS"]
    w_sum, w_total = 0, 0
    pw_sum, pw_total = 0, 0
    obs_temp = (st.get("obs") or {}).get("temperature_display")
    for model in models:
        a = acc.get(model, {})
        fcst = forecasts.get(model, {})
        raw = fcst.get("high")
        current_run = fcst.get("run", "")
        run_corr = (a.get("runs") or {}).get(current_run, {}).get("correction")
        overall_corr = a.get("correction")
        corr = run_corr if run_corr not in (None, "") else overall_corr
        try:
            mae = float(a.get("mae") or 0)
            adj = round(float(raw) + float(corr), 1) if raw is not None and corr not in (None, "") else None
            if mae > 0 and adj is not None:
                w = 1/mae; w_sum += adj*w; w_total += w
        except: pass
        # Pace
        try:
            current_fcst = fcst.get("current_fcst")
            pace = round(float(obs_temp) - float(current_fcst), 2) if obs_temp and current_fcst else None
            mae = float(a.get("mae") or 0)
            if mae > 0 and pace is not None:
                w = 1/mae; pw_sum += float(pace)*w; pw_total += w
        except: pass
    consensus = round(w_sum/w_total, 1) if w_total > 0 else None
    cons_pace = round(pw_sum/pw_total, 2) if pw_total > 0 else None
    implied = round(consensus + cons_pace, 1) if consensus is not None and cons_pace is not None else None
    if consensus is None:
        return
    entry = {
        "time": time_str,
        "consensus": consensus,
        "implied": implied,
        "pace": cons_pace,
        "obs": float(obs_temp) if obs_temp else None,
    }
    # Store in memory
    snaps = st["consensus_snapshots"]
    # Keep only today's
    snaps = [s for s in snaps if s.get("date") == date_str]
    entry["date"] = date_str
    snaps.append(entry)
    st["consensus_snapshots"] = snaps[-48:]  # max 48 entries (24hrs @ 30min)
    # Persist to disk
    try:
        ensure_data_dir()
        path = f"{DATA_DIR}/consensus_{station}.json"
        disk = load_json_file(path, {})
        if date_str not in disk:
            disk[date_str] = []
        disk[date_str].append(entry)
        # Keep last 90 days
        keys = sorted(disk.keys())
        if len(keys) > 90:
            for k in keys[:-90]: del disk[k]
        save_json_file(path, disk)
    except Exception as e:
        add_log(f"Consensus snapshot error: {e}", "warn", station)

def background_loop():
    while True:
        try:
            scheduled_fetch()
        except Exception as e:
            print(f"Loop error: {e}")
        try:
            now = okc_local_now()
            if now.hour == 1:
                for station in STATIONS:
                    rollup_daily_history(station)
        except Exception as e:
            print(f"Rollup error: {e}")
        time.sleep(REFRESH_SEC)

# ── Routes ────────────────────────────────────────────────────────────────────
def _get_prev_days(n, station="KOKC"):
    history = get_history(station)
    keys = sorted(history.keys(), reverse=True)[:n]
    return [{"date": k, "avg_pace": history[k]["avg_pace"], "snapshot_count": history[k].get("snapshot_count",0)} for k in keys]

@app.route("/api/state")
def api_state():
    station = request.args.get("station", "KOKC").upper()
    if station not in STATIONS:
        station = "KOKC"
    st = get_state(station)
    acc = st["accuracy"]
    models = active_models(station)
    rows = []
    for i, model in enumerate(models):
        a = acc.get(model, {})
        fcst = st["forecasts"].get(model, {})
        raw = fcst.get("high")
        # Use run-specific correction if available, fall back to overall correction
        current_run = fcst.get("run","")  # e.g. "11Z"
        run_corr = (a.get("runs") or {}).get(current_run, {}).get("correction")
        overall_corr = a.get("correction")
        corr = run_corr if (run_corr not in (None,"")) else overall_corr
        try: adj = round(float(raw) + float(corr), 1) if raw is not None and corr not in (None,"") else None
        except: adj = None
        obs_temp = (st["obs"] or {}).get("temperature_display")
        current_fcst = fcst.get("current_fcst")
        try: pace = round(float(obs_temp) - float(current_fcst), 1) if obs_temp and current_fcst else None
        except: pace = None
        # Tomorrow
        tmr_raw = fcst.get("tmr_high")
        tmr_low = fcst.get("tmr_low")
        tmr_low_time = fcst.get("tmr_low_time")
        try: tmr_adj = round(float(tmr_raw) + float(corr), 1) if tmr_raw is not None and corr not in (None,"") else tmr_raw
        except: tmr_adj = tmr_raw
        try: tmr_low_adj = round(float(tmr_low) + float(corr), 1) if tmr_low is not None and corr not in (None,"") else tmr_low
        except: tmr_low_adj = tmr_low

        rows.append({
            "rank": i+1, "model": model,
            "run": fcst.get("run","—"),
            "raw_high": raw, "correction": corr,
            "corr_source": "run" if (run_corr not in (None,"")) else "overall",
            "adj_high": adj, "pace": pace,
            "tmr_high": tmr_raw, "tmr_adj": tmr_adj,
            "tmr_low": tmr_low, "tmr_low_adj": tmr_low_adj, "tmr_low_time": tmr_low_time,
            "mae": a.get("mae"), "rmse": a.get("rmse"),
            "runs": a.get("runs", {}),
        })
    # Today consensus
    w_sum, w_total = 0, 0
    for r in rows:
        try:
            mae = float(r["mae"]); adj = r["adj_high"] if r["adj_high"] is not None else r["raw_high"]
            if mae > 0 and adj is not None:
                w = 1/mae; w_sum += adj*w; w_total += w
        except: pass
    consensus = round(w_sum/w_total, 1) if w_total > 0 else None
    # MAE-weighted consensus pace
    pw_sum, pw_total = 0, 0
    for r in rows:
        try:
            mae = float(r["mae"])
            pace = r["pace"]
            if mae > 0 and pace is not None:
                w = 1/mae; pw_sum += float(pace)*w; pw_total += w
        except: pass
    consensus_pace = round(pw_sum/pw_total, 2) if pw_total > 0 else None
    # Tomorrow consensus
    tw_sum, tw_total = 0, 0
    for r in rows:
        try:
            mae = float(r["mae"]); tadj = r["tmr_adj"] if r["tmr_adj"] is not None else r["tmr_high"]
            if mae > 0 and tadj is not None:
                w = 1/mae; tw_sum += tadj*w; tw_total += w
        except: pass
    tmr_consensus = round(tw_sum/tw_total, 1) if tw_total > 0 else None
    return jsonify({
        "station": station, "obs": st["obs"], "wethr_high": st["wethr_high"],
        "rows": rows, "consensus": consensus,
        "last_updated": st["last_updated"], "errors": st["errors"],
        "log": list(st["log"])[:30], "models": active_models(station),
        "nws_versions": st["nws_versions"],
        "tmr_consensus": tmr_consensus,
        "consensus_pace": consensus_pace,
        "today_avg_pace": st["today_avg_pace"],
        "today_snapshot_count": len(get_pacing(station).get(okc_local_now().strftime("%Y-%m-%d"), [])),
        "prev_days": _get_prev_days(3, station),
    })

@app.route("/api/history")
def api_history():
    station = request.args.get("station", "KOKC").upper()
    if station not in STATIONS:
        station = "KOKC"
    return jsonify(get_history(station))

@app.route("/api/accuracy", methods=["POST"])
def save_accuracy():
    station = request.args.get("station", "KOKC").upper()
    if station not in STATIONS:
        station = "KOKC"
    get_state(station)["accuracy"] = request.json or {}
    add_log("Accuracy data updated", "ok", station)
    return jsonify({"ok": True})


@app.route("/api/consensus_snapshots")
def api_consensus_snapshots():
    station = request.args.get("station", "KOKC").upper()
    if station not in STATIONS: station = "KOKC"
    st = get_state(station)
    today = okc_local_now().strftime("%Y-%m-%d")
    disk = load_json_file(f"{DATA_DIR}/consensus_{station}.json", {})
    return jsonify({
        "today": st.get("consensus_snapshots", []),
        "history": disk,
        "station": station,
    })

@app.route("/api/refresh", methods=["POST"])
def manual_refresh():
    station = request.args.get("station", "KOKC").upper()
    if station not in STATIONS:
        station = "KOKC"
    threading.Thread(target=fetch_all, args=(station,), daemon=True).start()
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
    <div class="stat-pill">
      <div class="lbl">Tmr Consensus</div>
      <div class="val" id="h-tmr" style="color:#a78bfa">--</div>
      <div class="sub2">MAE-weighted</div>
    </div>
    <div class="sp"></div>
    <div style="display:flex;gap:6px;align-items:center">
      <button id="btn-KOKC" onclick="switchStation('KOKC')" style="background:#1e40af;border:1px solid #3b82f6;color:#93c5fd;border-radius:4px;padding:5px 12px;font-size:11px;cursor:pointer;font-family:inherit;letter-spacing:1px">KOKC</button>
      <button id="btn-KPHL" onclick="switchStation('KPHL')" style="background:none;border:1px solid #334155;color:#64748b;border-radius:4px;padding:5px 12px;font-size:11px;cursor:pointer;font-family:inherit;letter-spacing:1px">KPHL</button>
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
  <button onclick="showTab('snapshots',this);loadSnapshots();">&#128248; Snapshots</button>
</nav>

<main>

<!-- DASHBOARD -->
<div class="tab active" id="tab-dashboard">
  <div class="srow">
    <div class="sc"><div class="lbl">Current Temp</div><div class="v" id="s-obs" style="color:var(--yellow)">--</div><div class="s" id="s-obs-t">awaiting</div></div>
    <div class="sc"><div class="lbl">Wethr High</div><div class="v" id="s-wh" style="color:var(--green)">--</div><div class="s">NWS trading day</div></div>
    <div class="sc"><div class="lbl">Consensus High</div><div class="v" id="s-con" style="color:var(--blue)">--</div><div class="s">MAE-weighted adj</div></div>
    <div class="sc"><div class="lbl">Models Live</div><div class="v" id="s-mods" style="color:var(--purple)">--</div><div class="s">forecast runs</div></div>
    <div class="sc"><div class="lbl">Tmr Consensus</div><div class="v" id="s-tmr" style="color:#a78bfa">--</div><div class="s">MAE-weighted adj</div></div>
  </div>

  <div class="card">
    <div class="ctitle">
      Top 10 Models &mdash; Live Forecasts + Accuracy Adjustments
      <span class="pill-y" id="acc-badge" style="display:none">Enter accuracy in Morning Entry</span>
      <span class="pill-g" id="acc-loaded" style="display:none">Accuracy loaded</span>
    </div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr><th>#</th><th>Model</th><th>Run</th><th>Fcst High</th><th>Correction</th><th>Adj High</th><th>Obs Pace</th><th>Tmr High</th><th>Tmr Adj</th><th>Tmr Low</th><th>Low Adj</th><th>Low Time</th><th>MAE</th><th>RMSE</th></tr></thead>
        <tbody id="main-tbody"></tbody>
      </table>
    </div>
  </div>

  <div class="card" id="pace-card" style="display:none">
    <div class="ctitle">Model Pacing vs Current Obs (<span id="pace-obs">--</span>F)</div>
    <div class="pbars" id="pbars"></div>
    <div style="font-size:10px;color:var(--dimmer);margin-top:10px">Pace = current obs minus model forecast for this hour</div>
  </div>

  <div class="card" id="cons-pace-card" style="display:none">
    <div class="ctitle">MAE-Weighted Consensus Pace</div>
    <div style="display:flex;align-items:baseline;gap:12px;flex-wrap:wrap">
      <div style="font-size:32px;font-weight:700" id="cons-pace-val">--</div>
      <div style="color:var(--dim);font-size:12px;line-height:1.6">
        MAE-weighted average of all model obs paces.<br>
        Apply to consensus high at your discretion.
      </div>
    </div>
    <div style="margin-top:10px;font-size:11px;color:var(--dim)">
      Implied adjusted high: <span id="cons-pace-implied" style="color:var(--green);font-weight:600">--</span>
    </div>
  </div>

  <div class="card" id="avg-pace-card">
    <div class="ctitle">Today's Rolling Average Pace (since 1AM)</div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr><th>Model</th><th>Avg Pace</th><th>Snapshots</th></tr></thead>
        <tbody id="avg-pace-tbody"><tr><td colspan="3" style="color:var(--dim)">Accumulating data...</td></tr></tbody>
      </table>
    </div>
  </div>

  <div class="card" id="prev-days-card">
    <div class="ctitle">Previous 3 Days Average Pace</div>
    <div style="overflow-x:auto"><table><thead id="prev-days-thead"></thead><tbody id="prev-days-tbody"><tr><td style="color:var(--dim)">No history yet</td></tr></tbody></table></div>
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


<!-- SNAPSHOTS TAB -->
<div class="tab" id="tab-snapshots">
  <div class="card">
    <div class="ctitle">Today&#39;s Consensus High Snapshots <span style="color:var(--dim);font-size:10px">(every 30 min, 6AM-10PM)</span></div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr><th>Time</th><th>Consensus High</th><th>Implied Adj High</th><th>Pace Adj</th><th>Obs</th></tr></thead>
        <tbody id="snap-tbody"><tr><td colspan="5" style="color:var(--dim)">No snapshots yet today.</td></tr></tbody>
      </table>
    </div>
  </div>
  <div class="card">
    <div class="ctitle">Historical Consensus Snapshots</div>
    <div style="display:flex;gap:8px;align-items:center;margin-bottom:12px;flex-wrap:wrap">
      <select id="snap-date-select" style="background:var(--bg);border:1px solid var(--border);color:var(--text);padding:5px 8px;border-radius:4px;font-family:inherit;font-size:12px" onchange="loadSnapshotDate()">
        <option value="">Select date...</option>
      </select>
    </div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr><th>Time</th><th>Consensus High</th><th>Implied Adj High</th><th>Pace Adj</th><th>Obs</th></tr></thead>
        <tbody id="snap-hist-tbody"><tr><td colspan="5" style="color:var(--dim)">Select a date above.</td></tr></tbody>
      </table>
    </div>
  </div>
</div>

</main>

<script>
var STATION = localStorage.getItem("active_station") || "KOKC";
var MODELS = [];
var accData = {};
try { accData = JSON.parse(localStorage.getItem("acc_"+STATION) || "{}"); } catch(e){}
if(Object.keys(accData).length) MODELS = Object.keys(accData).filter(function(m){ return m !== "NWS"; });
var countdown = 300;
var countdownTimer;

function clearDisplay(){
  ["h-obs","h-wh","h-con","h-tmr","s-obs","s-wh","s-con","s-tmr"].forEach(function(id){
    var el = document.getElementById(id); if(el) el.textContent="--";
  });
  ["h-obs-t","s-obs-t"].forEach(function(id){
    var el = document.getElementById(id); if(el) el.textContent="awaiting";
  });
  var tbody = document.getElementById("main-tbody"); if(tbody) tbody.innerHTML="";
  var pbars = document.getElementById("pbars"); if(pbars) pbars.innerHTML="";
  var pace = document.getElementById("pace-card"); if(pace) pace.style.display="none";
  var avg = document.getElementById("avg-pace-tbody");
  if(avg) avg.innerHTML='<tr><td colspan="3" style="color:var(--dim)">Accumulating...</td></tr>';
  document.getElementById("stxt").textContent="Switching...";
}

function switchStation(s){
  STATION = s;
  localStorage.setItem("active_station", s);
  clearDisplay();
  try { accData = JSON.parse(localStorage.getItem("acc_"+s) || "{}"); } catch(e){ accData = {}; }
  MODELS = Object.keys(accData).filter(function(m){ return m !== "NWS"; });
  ["KOKC","KPHL"].forEach(function(st){
    var btn = document.getElementById("btn-"+st);
    if(st === s){
      btn.style.background="#1e40af"; btn.style.borderColor="#3b82f6"; btn.style.color="#93c5fd";
    } else {
      btn.style.background="none"; btn.style.borderColor="#334155"; btn.style.color="#64748b";
    }
  });
  var names = {"KOKC":"Oklahoma City Will Rogers World Airport","KPHL":"Philadelphia International Airport"};
  document.querySelector(".sub").textContent = names[s] || s;
  document.querySelector("h1").textContent = s + " · Model Tracker";
  buildForms(); renderPreview(); poll();
}

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
    localStorage.setItem("acc_"+STATION, JSON.stringify(parsed));
    localStorage.setItem("acc_"+STATION+"_time", new Date().toLocaleString());
    fetch("/api/accuracy?station="+STATION,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(parsed)})
      .then(function(r){
        if(!r.ok) throw new Error("HTTP "+r.status);
        status.style.color="var(--green)";
        status.textContent = "Loaded "+keys.length+" models at "+new Date().toLocaleTimeString();
        document.getElementById("json-paste").value="";
        buildForms(); renderPreview(); poll();
      }).catch(function(e){
        status.style.color="var(--yellow)";
        status.textContent = "Saved locally (server: "+e.message+"). Will sync on next refresh.";
        buildForms(); renderPreview();
      });
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
  localStorage.setItem("acc_"+STATION, JSON.stringify(data));
  fetch("/api/accuracy?station="+STATION,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(data)})
    .then(function(){ document.getElementById("save-status").textContent="Saved "+new Date().toLocaleTimeString(); });
}

function clearAccuracy(){
  if(!confirm("Clear all accuracy data?")) return;
  accData = {}; MODELS = [];
  localStorage.removeItem("acc_"+STATION); localStorage.removeItem("acc_"+STATION+"_time");
  fetch("/api/accuracy?station="+STATION,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({})});
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
  var t = localStorage.getItem("acc_"+STATION+"_time");
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
  var tmrCon = data.tmr_consensus;
  if(tmrCon){
    document.getElementById("h-tmr").textContent=tmrCon+"F";
    document.getElementById("s-tmr").textContent=tmrCon+"F";
  }
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
      +'<td style="color:#a78bfa">'+(r.tmr_high!=null?r.tmr_high+"F":"--")+'</td>'
      +'<td style="color:#c4b5fd;font-weight:600">'+(r.tmr_adj!=null?r.tmr_adj+"F":"--")+'</td>'
      +'<td style="color:#60a5fa">'+(r.tmr_low!=null?r.tmr_low+"F":"--")+'</td>'
      +'<td style="color:#93c5fd;font-weight:600">'+(r.tmr_low_adj!=null?r.tmr_low_adj+"F":"--")+'</td>'
      +'<td style="color:var(--dim);font-size:11px">'+(r.tmr_low_time||"--")+'</td>'
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

  // Consensus pace card
  var consPace = data.consensus_pace;
  var consPaceCard = document.getElementById("cons-pace-card");
  if(consPace != null && obs){
    consPaceCard.style.display = "block";
    var cpEl = document.getElementById("cons-pace-val");
    cpEl.textContent = (consPace >= 0 ? "+" : "") + consPace + "F";
    cpEl.style.color = consPace >= 0 ? "var(--green)" : "var(--red)";
    var implied = con ? (Math.round((parseFloat(con) + consPace) * 10) / 10) + "F" : "--";
    document.getElementById("cons-pace-implied").textContent = implied;
  } else {
    consPaceCard.style.display = "none";
  }

  // Today avg pace
  var avgPace = data.today_avg_pace || {};
  var avgModels = Object.keys(avgPace);
  var todaySnaps = data.today_snapshot_count || 0;
  if(avgModels.length){
    document.getElementById("avg-pace-tbody").innerHTML = avgModels.map(function(m,i){
      var p = avgPace[m];
      var bg = i%2?"background:#0a1018":"";
      var pc = paceColor(p);
      return '<tr style="'+bg+'"><td style="color:#e8f0f8;font-weight:600">'+m+'</td>'
        +'<td style="color:'+pc+';font-weight:600">'+(p>=0?"+":"")+p.toFixed(2)+'F</td>'
        +'<td style="color:var(--dim)">'+todaySnaps+'</td></tr>';
    }).join("");
  } else {
    document.getElementById("avg-pace-tbody").innerHTML = '<tr><td colspan="3" style="color:var(--dim)">Accumulating — updates every 5 min</td></tr>';
  }

  // Prev 3 days
  var prevDays = data.prev_days || [];
  if(prevDays.length){
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
  } else {
    document.getElementById("prev-days-thead").innerHTML = '';
    document.getElementById("prev-days-tbody").innerHTML = '<tr><td style="color:var(--dim)">No history yet — builds after first full day</td></tr>';
  }

  // Status
  document.getElementById("sdot").className = "dot "+(data.errors&&data.errors.length?"dot-yellow":"dot-green");
  document.getElementById("stxt").textContent = data.last_updated?"Updated "+data.last_updated.slice(11,16):"Live";
}

// KEY FIX: chain /api/state after /api/accuracy completes so MAE values are present
function poll(){
  try { accData = JSON.parse(localStorage.getItem("acc_"+STATION) || "{}"); } catch(e){ accData = {}; }
  var stateUrl = "/api/state?station="+STATION;
  function fetchState(){
    fetch(stateUrl).then(function(r){ return r.json(); }).then(render).catch(function(e){ console.error(e); });
  }
  if(Object.keys(accData).length){
    fetch("/api/accuracy?station="+STATION, {
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify(accData)
    }).then(fetchState).catch(fetchState);
  } else {
    fetchState();
  }
}

function manualRefresh(){
  fetch("/api/refresh?station="+STATION,{method:"POST"});
  countdown=300;
  document.getElementById("stxt").textContent="Fetching...";
  setTimeout(poll,8000);
  setTimeout(poll,15000);
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

document.addEventListener("visibilitychange", function(){
  if(document.visibilityState === "visible"){ poll(); }
});
window.addEventListener("focus", function(){ poll(); });

var _snapData = {};

function loadSnapshots(){
  fetch("/api/consensus_snapshots?station="+STATION)
    .then(function(r){ return r.json(); })
    .then(function(data){
      _snapData = data.history || {};
      var today = data.today || [];
      var tbody = document.getElementById("snap-tbody");
      if(today.length){
        tbody.innerHTML = today.slice().reverse().map(function(s,i){
          var bg = i%2?"background:#0a1018":"";
          var pc = s.pace!=null?(s.pace>=0?"var(--red)":"var(--green)"):"var(--dim)";
          var paceStr = s.pace!=null?(s.pace>=0?"+":"")+s.pace+"F":"--";
          return '<tr style="'+bg+'">'
            +'<td style="color:var(--dim)">'+s.time+'</td>'
            +'<td style="color:var(--blue);font-weight:600">'+(s.consensus!=null?s.consensus+"F":"--")+'</td>'
            +'<td style="color:var(--green);font-weight:600">'+(s.implied!=null?s.implied+"F":"--")+'</td>'
            +'<td style="color:'+pc+'">'+paceStr+'</td>'
            +'<td style="color:var(--yellow)">'+(s.obs!=null?s.obs+"F":"--")+'</td>'
            +'</tr>';
        }).join("");
      } else {
        tbody.innerHTML = '<tr><td colspan="5" style="color:var(--dim)">No snapshots yet today.</td></tr>';
      }
      var dates = Object.keys(_snapData).sort().reverse();
      var sel = document.getElementById("snap-date-select");
      sel.innerHTML = '<option value="">Select date...</option>' +
        dates.map(function(d){ return '<option value="'+d+'">'+d+'</option>'; }).join("");
    }).catch(function(e){ console.error("Snapshot load error",e); });
}

function loadSnapshotDate(){
  var date = document.getElementById("snap-date-select").value;
  var tbody = document.getElementById("snap-hist-tbody");
  if(!date || !_snapData[date]){
    tbody.innerHTML = '<tr><td colspan="5" style="color:var(--dim)">No data for this date.</td></tr>';
    return;
  }
  var snaps = _snapData[date].slice().reverse();
  tbody.innerHTML = snaps.map(function(s,i){
    var bg = i%2?"background:#0a1018":"";
    var pc = s.pace!=null?(s.pace>=0?"var(--red)":"var(--green)"):"var(--dim)";
    var paceStr = s.pace!=null?(s.pace>=0?"+":"")+s.pace+"F":"--";
    return '<tr style="'+bg+'">'
      +'<td style="color:var(--dim)">'+s.time+'</td>'
      +'<td style="color:var(--blue);font-weight:600">'+(s.consensus!=null?s.consensus+"F":"--")+'</td>'
      +'<td style="color:var(--green);font-weight:600">'+(s.implied!=null?s.implied+"F":"--")+'</td>'
      +'<td style="color:'+pc+'">'+paceStr+'</td>'
      +'<td style="color:var(--yellow)">'+(s.obs!=null?s.obs+"F":"--")+'</td>'
      +'</tr>';
  }).join("");
}

function loadHistory(){
  fetch("/api/history?station="+STATION).then(function(r){ return r.json(); }).then(function(history){
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
            print("Background loop started")

with app.app_context():
    start_background()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

        
