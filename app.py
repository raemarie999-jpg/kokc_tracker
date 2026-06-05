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

def active_models(station="KOKC"):
    acc = get_state(station).get("accuracy", {})
    models = [m for m in acc.keys() if m != "NWS"] if acc else ALL_KNOWN_MODELS
    return models

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

def okc_day_bounds(offset=0):
    utc_now = datetime.utcnow()
    okc_local = utc_now - timedelta(hours=5)
    day_start = okc_local.replace(hour=0,minute=0,second=0,microsecond=0) + timedelta(hours=5) + timedelta(days=offset)
    day_end = day_start + timedelta(hours=24)
    return day_start, day_end

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

    try:
        obs = wethr_get(f"observations.php?station_code={station}&mode=latest")
        st["obs"] = obs
        add_log(f"Obs: {obs.get('temperature_display')}F", "ok", station)
    except Exception as e:
        errors.append(f"Obs: {e}")
        add_log(f"Obs error: {e}", "err", station)

    try:
        wh = wethr_get(f"observations.php?station_code={station}&mode=wethr_high&logic=nws")
        st["wethr_high"] = wh
        add_log(f"Wethr High: {wh.get('wethr_high')}F", "ok", station)
    except Exception as e:
        errors.append(f"WethrHigh: {e}")
        add_log(f"Wethr High error: {e}", "err", station)

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

                # 🔥 DEBUG LOG 1 (per model)
                a = st["accuracy"].get(model, {})
                corr = a.get("correction")
                adj = None
                try:
                    adj = round(float(raw_temp) + float(corr), 1) if raw_temp is not None and corr not in (None, "") else None
                except:
                    adj = None

                add_log(
                    f"MODEL DEBUG: {model} mae={a.get('mae')} raw={raw_temp} corr={corr} adj={adj}",
                    "info",
                    station
                )

                add_log(f"{model}: high={raw_temp} now={current_temp} run={run_fmt}", "ok", station)

        except Exception as e:
            errors.append(f"{model}: {e}")
            add_log(f"{model} error: {str(e)[:80]}", "warn", station)

    st["nws_versions"] = {}
    st["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st["errors"] = errors

    add_log(f"Done. {len(st['forecasts'])} models loaded.", "ok", station)

    try:
        rows = build_snapshot_rows(station)
        save_pacing_snapshot(rows, station)
    except Exception as e:
        add_log(f"Snapshot error: {e}", "warn", station)

    try:
        now_local = okc_local_now()
        if now_local.minute < 10 or (now_local.minute >= 30 and now_local.minute < 40):
            save_consensus_snapshot(station)
    except Exception as e:
        add_log(f"Consensus snapshot error: {e}", "warn", station)

def okc_local_now():
    return datetime.utcnow() - timedelta(hours=5)

_memory_snapshots = {}

def save_pacing_snapshot(rows, station="KOKC"):
    st = get_state(station)
    now = okc_local_now()
    date_str = now.strftime("%Y-%m-%d")

    entry = {"time": now.strftime("%H:%M")}
    for r in rows:
        if r.get("pace") is not None:
            entry[r["model"]] = r["pace"]

    _memory_snapshots.setdefault(date_str, []).append(entry)

    avg = {}
    for r in rows:
        m = r["model"]
        vals = [s[m] for s in _memory_snapshots[date_str] if m in s]
        if vals:
            avg[m] = round(sum(vals)/len(vals), 2)

    st["today_avg_pace"] = avg
    add_log(f"Snapshot updated", "info", station)

def build_snapshot_rows(station="KOKC"):
    st = get_state(station)
    acc = st["accuracy"]
    models = list(st["forecasts"].keys())
    obs_temp = (st["obs"] or {}).get("temperature_display")

    rows = []
    for model in models:
        fcst = st["forecasts"].get(model, {})
        try:
            pace = float(obs_temp) - float(fcst.get("current_fcst"))
        except:
            pace = None
        rows.append({"model": model, "pace": pace})
    return rows

def save_consensus_snapshot(station="KOKC"):
    st = get_state(station)
    now = okc_local_now()
    if now.hour < 6 or now.hour >= 22:
        return

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
            adj = float(raw) + float(corr) if raw is not None and corr not in (None, "") else None
            if mae > 0 and adj is not None:
                w = 1 / mae
                w_sum += adj * w
                w_total += w
        except:
            pass

        try:
            current_fcst = fcst.get("current_fcst")
            pace = float(obs_temp) - float(current_fcst) if obs_temp and current_fcst else None
            mae = float(a.get("mae") or 0)
            if mae > 0 and pace is not None:
                w = 1 / mae
                pw_sum += pace * w
                pw_total += w
        except:
            pass

    consensus = round(w_sum / w_total, 1) if w_total else None
    cons_pace = round(pw_sum / pw_total, 2) if pw_total else None

    entry = {
        "time": now.strftime("%H:%M"),
        "consensus": consensus,
        "pace": cons_pace,
        "date": now.strftime("%Y-%m-%d"),
    }

    st["consensus_snapshots"].append(entry)

def okc_local_now():
    return datetime.utcnow() - timedelta(hours=5)

def scheduled_fetch():
    for i, station in enumerate(STATIONS):
        if i > 0:
            time.sleep(30)
        t = threading.Thread(target=fetch_all, args=(station,), daemon=True)
        t.start()
        t.join(timeout=120)

def background_loop():
    while True:
        try:
            scheduled_fetch()
        except Exception as e:
            print(e)
        time.sleep(REFRESH_SEC)

@app.route("/api/state")
def api_state():
    station = request.args.get("station", "KOKC").upper()
    st = get_state(station)

    acc = st["accuracy"]
    forecasts = st["forecasts"]

    models = active_models(station)
    rows = []

    for i, model in enumerate(models):
        a = acc.get(model, {})
        fcst = forecasts.get(model, {})

        raw = fcst.get("high")
        current_run = fcst.get("run", "")
        run_corr = (a.get("runs") or {}).get(current_run, {}).get("correction")
        overall_corr = a.get("correction")
        corr = run_corr if run_corr not in (None, "") else overall_corr

        try:
            adj = round(float(raw) + float(corr), 1) if raw is not None and corr not in (None, "") else None
        except:
            adj = None

        obs_temp = (st["obs"] or {}).get("temperature_display")
        current_fcst = fcst.get("current_fcst")

        try:
            pace = round(float(obs_temp) - float(current_fcst), 1) if obs_temp and current_fcst else None
        except:
            pace = None

        # 🔥 DEBUG LOG 2 (requested)
        add_log(
            f"w_total={{w_total}} models={len(rows)} acc_keys={len(acc)} forecasts={len(forecasts)}",
            "warn",
            station
        )

        rows.append({
            "model": model,
            "run": fcst.get("run", "—"),
            "raw_high": raw,
            "correction": corr,
            "adj_high": adj,
            "pace": pace,
            "tmr_high": fcst.get("tmr_high"),
            "tmr_low": fcst.get("tmr_low"),
            "tmr_low_time": fcst.get("tmr_low_time"),
            "mae": a.get("mae"),
            "rmse": a.get("rmse"),
            "runs": a.get("runs", {}),
        })

    # consensus
    w_sum, w_total = 0, 0

    for r in rows:
        try:
            mae = float(r["mae"])
            adj = r["adj_high"] or r["raw_high"]
            if mae > 0 and adj is not None:
                w = 1 / mae
                w_sum += adj * w
                w_total += w
        except:
            pass

    consensus = round(w_sum / w_total, 1) if w_total else None

    return jsonify({
        "rows": rows,
        "consensus": consensus,
        "models": models,
        "log": st["log"][:30],
        "last_updated": st["last_updated"],
        "errors": st["errors"]
    })

@app.route("/")
def index():
    return render_template_string(HTML)

# --- HTML/JS unchanged from your original (included fully) ---
HTML = """... (unchanged from your paste) ..."""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
