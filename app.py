import os, json, time, threading, random, math, fcntl
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, render_template_string
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024  # 2MB limit
API_KEY = os.environ.get("WETHR_API_KEY", "")
DATA_DIR = "/data"
PACING_FILE = f"{DATA_DIR}/pacing_snapshots.json"
HISTORY_FILE = f"{DATA_DIR}/daily_history.json"
_BGLOOP_LOCKFILE = f"{DATA_DIR}/bgloop.lock"

# --- Rate limiting: max requests per second to wethr API ---
_api_lock = threading.Lock()
_last_request_time = 0
MIN_REQUEST_INTERVAL = 2.5  # seconds between API calls

# --- Manual refresh cooldown: stops external pings / rapid re-clicks from
# bypassing REFRESH_SEC and spawning unlimited fetch_all() runs ---
_manual_refresh_lock = threading.Lock()
_last_manual_refresh = {}
MANUAL_REFRESH_COOLDOWN_SEC = 120  # min seconds between manual refreshes, per station

# --- FIX: prevent two concurrent fetch_all() runs for the same station ---
# manual_refresh() spawns a fresh thread to call fetch_all(station) directly,
# completely independent of bgloop's own scheduled_fetch() loop. Nothing
# previously stopped both from running for the same station at the same
# time. Since every wethr_get() call is serialized through the same global
# _api_lock/_throttle(), a second concurrent fetch_all() for one station
# doesn't error — it just silently queues every one of its model requests
# behind the other run's, each spaced MIN_REQUEST_INTERVAL apart, making the
# whole thing take 2x+ as long with no distinguishing log output (add_log
# doesn't tag which call/thread produced a line). That's a very plausible
# explanation for "fetch looked healthy, then went quiet for minutes."
# A simple per-station lock makes a second concurrent attempt for the same
# station back off immediately and log why, instead of silently queuing.
# (_fetch_locks itself is built just below, once STATION_POOL exists.)

# --- Daily API call counter: informational only — NO cap is enforced.
# Resets at 19:30 UTC (= 3:30pm EDT / 2:30pm EST) purely for display purposes
# in /api/quota, so you can see usage trends without anything ever blocking
# a fetch. ---
_CAP_RESET_UTC_HOUR = 19
_CAP_RESET_UTC_MINUTE = 30
_counter_lock = threading.Lock()

# --- BUG P1 FIX: single lock guarding background-thread startup so a storm of
# concurrent requests (page poll + visibilitychange + focus + manual refresh's
# 3 scheduled polls) can't each independently decide bgloop is missing and
# spawn a duplicate. We check once outside the lock (fast path, no contention
# in the common case), then re-check inside the lock before spawning. ---
_watchdog_lock = threading.Lock()


def _get_period_key():
    """Returns string key for the current counting period.
    Resets at 19:30 UTC (3:30pm EDT in summer; shifts 1hr in winter — acceptable).
    Used only to bucket the informational request counter — nothing is gated on it.
    """
    now = datetime.utcnow()
    reset_today = now.replace(hour=_CAP_RESET_UTC_HOUR, minute=_CAP_RESET_UTC_MINUTE, second=0, microsecond=0)
    period_start = reset_today if now >= reset_today else reset_today - timedelta(days=1)
    return period_start.strftime("%Y-%m-%d_%H%M")


def _load_api_counter():
    try:
        with open(f"{DATA_DIR}/api_counter_tracker.json") as f:
            return json.load(f)
    except:
        return {}


def _save_api_counter(data):
    try:
        ensure_data_dir()
        tmp = f"{DATA_DIR}/api_counter_tracker.json.tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, f"{DATA_DIR}/api_counter_tracker.json")
    except Exception as e:
        print(f"Counter save error: {e}")


def _check_and_increment():
    """Increments and saves the informational request counter for the
    current period. No cap is enforced — this never raises.
    This is the ONE place that should ever increment the counter. Call sites
    must not pre-check/duplicate this logic (see BUG P2)."""
    with _counter_lock:
        period = _get_period_key()
        data = _load_api_counter()
        count = data.get(period, 0)
        data[period] = count + 1
        keys = sorted(data.keys())
        if len(keys) > 3:
            for k in keys[:-3]:
                del data[k]
        _save_api_counter(data)
        return data[period]


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


STATION_POOL = {
    "KDCA": {"name": "Washington Reagan National Airport", "lon_w": 77.04, "tz": -5},
    "KOKC": {"name": "Oklahoma City Will Rogers World Airport", "lon_w": 97.60, "tz": -6},
    "KPHL": {"name": "Philadelphia International Airport", "lon_w": 75.24, "tz": -5},
    "KBOS": {"name": "Boston Logan International Airport", "lon_w": 71.01, "tz": -5},
    "KDEN": {"name": "Denver International Airport", "lon_w": 104.67, "tz": -7},
    "KHOU": {"name": "Houston Hobby Airport", "lon_w": 95.28, "tz": -6},
    "KLAS": {"name": "Las Vegas Harry Reid International", "lon_w": 115.15, "tz": -8},
    "KMDW": {"name": "Chicago Midway International Airport", "lon_w": 87.75, "tz": -6},
    "KMSP": {"name": "Minneapolis-St. Paul International", "lon_w": 93.22, "tz": -6},
    "KSAT": {"name": "San Antonio International Airport", "lon_w": 98.47, "tz": -6},
}
DEFAULT_ACTIVE_STATIONS = ["KOKC", "KPHL", "KDCA"]
_active_stations = None
_active_lock = threading.Lock()

# One lock per station so two concurrent fetch_all() calls for the SAME
# station (e.g. bgloop's scheduled run + a manual refresh firing at the same
# moment) can't silently queue behind each other for minutes — see comment
# near MANUAL_REFRESH_COOLDOWN_SEC above for the full explanation.
_fetch_locks = {s: threading.Lock() for s in STATION_POOL}


def load_active_stations():
    global _active_stations
    saved = load_json_file(f"{DATA_DIR}/active_stations.json", None)
    if isinstance(saved, list):
        valid = [s for s in saved if s in STATION_POOL]
        if len(valid) == 3:
            _active_stations = valid
            return
    _active_stations = list(DEFAULT_ACTIVE_STATIONS)


def get_active_stations():
    if _active_stations is None:
        load_active_stations()
    return list(_active_stations)


def set_active_stations(stations):
    global _active_stations
    valid = [s for s in stations if s in STATION_POOL]
    if len(valid) != 3:
        return False
    with _active_lock:
        _active_stations = valid
    save_json_file(f"{DATA_DIR}/active_stations.json", valid)
    return True


# --- Solar-adjusted nowcast high ---
BOOST_BASE = 4.0  # midpoint of 3-5°F climatological range (clear summer day)
SKY_BOOST_FACTOR = {
    "SKC": 1.0, "CLR": 1.0, "CAVOK": 1.0, "NSC": 1.0, "FEW": 1.0,
    "SCT": 0.6,
    "BKN": 0.25,
    "OVC": 0.05, "OVX": 0.05, "VV": 0.05,
}
NORTHERLY_DIRS = {"N", "NNE", "NNW", "NE", "NW"}  # suppress boost if METAR wind is any of these
ALL_KNOWN_MODELS = [
    "ARPEGE", "HRRR", "UKMO", "LAV-MOS", "NAM", "RAP", "GEM-GDPS", "NAM-MOS", "NBM",
    "NAM4KM", "GFS", "ICON", "GFS-MOS", "ECMWF-HRES", "GEFS", "JMA", "RDPS", "SREF"
]
RUN_CYCLES = ["00Z", "01Z", "02Z", "03Z", "04Z", "05Z", "06Z", "07Z", "08Z", "09Z", "10Z", "11Z",
              "12Z", "13Z", "14Z", "15Z", "16Z", "17Z", "18Z", "19Z", "20Z", "21Z", "22Z", "23Z"]
REFRESH_SEC = 1800  # 30 min between auto-refresh cycles; use NOW button for on-demand update


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
        "metar": None,
        "solar_noon_obs": None,  # obs temp recorded closest to solar noon
        "solar_noon_dt": None,   # UTC datetime of that obs
    }


states = {s: make_state() for s in STATION_POOL}


def get_state(station=None):
    if station and station in states:
        return states[station]
    active = get_active_stations()
    return states.get(active[0], states[DEFAULT_ACTIVE_STATIONS[0]])


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


def _throttle():
    """Enforce minimum interval between API calls (global, across all stations).

    BUG FIX: the original code held _api_lock across the entire sleep(),
    meaning the lock was monopolised for up to 2.5 s × 54 calls = 135 s per
    fetch cycle. Nothing in the request-handling path contends for _api_lock
    directly, but holding it across a blocking sleep is still wrong: any
    future code path that acquires it would deadlock for the full sleep
    duration. Fix: compute the required wait inside the lock (atomic read of
    _last_request_time + update), then sleep outside the lock.
    """
    global _last_request_time
    with _api_lock:
        now = time.monotonic()
        gap = now - _last_request_time
        wait = MIN_REQUEST_INTERVAL - gap if gap < MIN_REQUEST_INTERVAL else 0
        _last_request_time = time.monotonic() + wait  # reserve the slot now
    if wait > 0:
        time.sleep(wait)


def wethr_get(path, retries=3):
    """
    Rate-limited GET with exponential backoff retry on 429/5xx.
    No daily cap is enforced — _check_and_increment() only tallies usage for
    the informational /api/quota endpoint and never blocks a request.
    This is the ONLY place that calls _check_and_increment() — see BUG P2.
    """
    _check_and_increment()  # tallies usage only; never raises / never blocks
    for attempt in range(retries):
        _throttle()
        try:
            r = requests.get(
                f"https://wethr.net/api/v2/{path}",
                headers={"X-API-Key": API_KEY},
                timeout=10
            )
            if r.status_code == 429:
                wait = (2 ** attempt) * 5 + random.uniform(1, 3)
                print(f"[429] Rate limited on {path}. Waiting {wait:.1f}s (attempt {attempt+1})")
                time.sleep(wait)
                continue
            if r.status_code == 400:
                # --- DIAGNOSTIC FIX ---
                # raise_for_status() alone only gives "400 Client Error: Bad
                # Request for url: ..." — it discards the response body,
                # which is usually where the API explains *why* (bad param
                # name, model not available for this station, etc). Surface
                # that body in the raised error and in the logs so 400s are
                # actually debuggable instead of just "Bad Request".
                body = (r.text or "")[:300]
                print(f"[400] {path} -> {body}", flush=True)
                raise requests.exceptions.HTTPError(
                    f"400 Client Error: {body or '(empty body)'} for url: {r.url}",
                    response=r
                )
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if attempt < retries - 1 and e.response is not None and e.response.status_code in (429, 500, 502, 503, 504):
                wait = (2 ** attempt) * 5 + random.uniform(1, 3)
                print(f"[{e.response.status_code}] Retrying {path} in {wait:.1f}s")
                time.sleep(wait)
                continue
            raise
        except requests.exceptions.RequestException:
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
                continue
            raise
    raise RuntimeError(f"Failed after {retries} attempts: {path}")


def get_temp(x):
    # Exhaustive key list covering known wethr API variants
    for k in ["temperature_f", "temperature_display", "temperature", "temp", "value", "high",
              "max_temp", "max_temperature", "temp_f", "temp_max", "forecast_high",
              "temperature_high", "t", "fahrenheit", "f"]:
        v = x.get(k)
        if v is not None:
            try:
                return round(float(v), 1)
            except:
                pass
    # Last resort: find any numeric-looking value in the dict that's plausibly a temp
    for k, v in x.items():
        if k in ("valid_time", "run_time", "run", "model", "station", "date", "time", "hour", "id"):
            continue
        if v is not None and not isinstance(v, (dict, list)):
            try:
                f = float(v)
                if 0 < f < 130:  # plausible Fahrenheit range
                    return round(f, 1)
            except:
                pass
    return None


def parse_vt(x):
    vt = str(x.get("valid_time", ""))
    try:
        return datetime.strptime(vt[:16], "%Y-%m-%d %H:%M")
    except:
        return None


def station_local_now(station="KOKC"):
    offset = STATION_POOL.get(station, {}).get("tz", -6)
    return datetime.utcnow() + timedelta(hours=offset)


def station_day_bounds(station="KOKC", offset=0):
    tz_offset = STATION_POOL.get(station, {}).get("tz", -6)
    local_now = datetime.utcnow() + timedelta(hours=tz_offset)
    local_midnight = local_now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=offset)
    day_start_utc = local_midnight - timedelta(hours=tz_offset)
    day_end_utc = day_start_utc + timedelta(hours=24)
    return day_start_utc, day_end_utc


def today_entries(temps, station="KOKC"):
    day_start, day_end = station_day_bounds(station, 0)
    filtered = [
        x for x in temps
        if parse_vt(x) is not None
        and day_start <= parse_vt(x) < day_end
    ]
    add_log(
        f"today_entries: {station} total={len(temps)} filtered={len(filtered)} "
        f"window={day_start} -> {day_end}",
        "info",
        station
    )
    return filtered if filtered else temps


def tomorrow_entries(temps, station="KOKC"):
    day_start, day_end = station_day_bounds(station, 1)
    filtered = [x for x in temps if parse_vt(x) is not None and day_start <= parse_vt(x) < day_end]
    return filtered


def fmt_run(run_raw):
    try:
        if len(run_raw) >= 13:
            return run_raw[11:13] + "Z"
        return run_raw or "—"
    except:
        return "—"


def get_run_data(acc_model, run_key):
    """
    Look up run-specific accuracy data for a model.
    Priority: exact run match -> 'default' fallback -> empty dict.
    Returns (run_data_dict, source_label) where source_label is 'run', 'default', or 'overall'.
    """
    runs = acc_model.get("runs") or {}
    # 1. Exact run match
    rd = runs.get(run_key, {})
    if rd and (rd.get("mae") or rd.get("correction") not in (None, "")):
        return rd, "run"
    # 2. Default fallback run
    default_rd = runs.get("default", {})
    if default_rd and (default_rd.get("mae") or default_rd.get("correction") not in (None, "")):
        return default_rd, "default"
    # 3. Nothing run-specific found
    return {}, "overall"


def deg_to_cardinal(deg):
    """Convert wind direction degrees to 16-point cardinal direction."""
    if deg is None:
        return None
    dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    return dirs[round(float(deg) / (360 / len(dirs))) % len(dirs)]


def _sf(v):
    """Safe float: returns rounded float or None. Guards against NaN/Infinity."""
    try:
        if v is None:
            return None
        result = round(float(v), 1)
        if math.isnan(result) or math.isinf(result):
            return None
        return result
    except:
        return None


_SKY_LABELS = {
    "SKC": "Clear", "CLR": "Clear", "CAVOK": "Clear", "NSC": "Clear",
    "FEW": "Few", "SCT": "Scattered", "BKN": "Broken", "OVC": "Overcast", "OVX": "Obscured", "VV": "Obscured",
}
_SKY_PRIORITY = {"SKC": 0, "CLR": 0, "CAVOK": 0, "NSC": 0, "FEW": 1, "SCT": 2, "BKN": 3, "OVC": 4, "OVX": 4, "VV": 4}


def fetch_metar(station):
    """Fetch latest METAR from aviationweather.gov — free, no key, separate from wethr budget."""
    url = f"https://aviationweather.gov/api/data/metar?ids={station}&format=json&hours=2"
    r = requests.get(url, timeout=8, headers={"User-Agent": "WeatherTracker/1.0"})
    r.raise_for_status()
    data = r.json()
    if not data:
        return None
    m = data[0]
    # Sky layers
    sky_layers = m.get("sky") or []
    cover_label = "CLR"
    ceiling_ft = None
    sky_parts = []
    for layer in sky_layers:
        cov = layer.get("cover", "")
        base = layer.get("base")
        sky_parts.append(f"{cov}{str(int(base/100)).zfill(3) if base is not None else '///'}")
        if _SKY_PRIORITY.get(cov, 0) > _SKY_PRIORITY.get(cover_label, 0):
            cover_label = cov
        if cov in ("BKN", "OVC", "OVX", "VV") and base is not None:
            if ceiling_ft is None or int(base) < ceiling_ft:
                ceiling_ft = int(base)

    def kt_mph(v):
        try:
            return round(float(v) * 1.15078, 1)
        except:
            return None

    def c_to_f(c):
        try:
            return round(float(c) * 9 / 5 + 32, 1)
        except:
            return None

    wdir = m.get("wdir")
    if wdir == 360:
        wdir = 0
    return {
        "sky_cover": cover_label,
        "sky_label": _SKY_LABELS.get(cover_label, cover_label),
        "sky_layers": sky_parts,
        "ceiling_ft": ceiling_ft,
        "wind_dir_deg": wdir,
        "wind_dir_card": deg_to_cardinal(wdir),
        "wind_speed_kt": m.get("wspd"),
        "wind_speed_mph": kt_mph(m.get("wspd")),
        "wind_gust_kt": m.get("wgst"),
        "wind_gust_mph": kt_mph(m.get("wgst")),
        "visibility_sm": m.get("visib"),
        "temp_f": c_to_f(m.get("temp")),
        "dew_f": c_to_f(m.get("dewp")),
        "obs_time_utc": m.get("reportTime"),
        "raw": m.get("rawOb"),
    }


def solar_noon_utc(lon_deg_west, date=None):
    """Returns solar noon as a UTC datetime for the given longitude (degrees West)."""
    if date is None:
        date = datetime.utcnow().date()
    doy = date.timetuple().tm_yday
    B = math.radians(360 / 365 * (doy - 81))
    eot_min = 9.87 * math.sin(2 * B) - 7.53 * math.cos(B) - 1.5 * math.sin(B)
    solar_noon_h = 12.0 + lon_deg_west / 15.0 - eot_min / 60.0
    h = int(solar_noon_h)
    frac = solar_noon_h - h
    m = int(frac * 60)
    s = int((frac * 60 - m) * 60)
    return datetime(date.year, date.month, date.day, max(0, min(23, h)), m, s)


def compute_nowcast(station):
    """
    Solar-adjusted nowcast high.
    Uses recorded solar noon obs + tiered sky boost scaled by METAR wind direction flag.
    Returns None if solar noon obs hasn't been recorded yet today.
    """
    st = get_state(station)
    solar_noon_obs = st.get("solar_noon_obs")
    if solar_noon_obs is None:
        return None
    metar = st.get("metar")
    sky_cover = metar.get("sky_cover", "CLR") if metar else "CLR"
    wind_card = metar.get("wind_dir_card") if metar else None
    if wind_card in NORTHERLY_DIRS:
        boost = round(BOOST_BASE * 0.05, 1)
        note = f"N wind suppressed ({wind_card})"
        suppressed = True
    else:
        sky_factor = SKY_BOOST_FACTOR.get(sky_cover, 1.0)
        boost = round(BOOST_BASE * sky_factor, 1)
        note = f"{_SKY_LABELS.get(sky_cover, sky_cover)} ({int(sky_factor*100)}%)"
        suppressed = False
    nowcast = round(solar_noon_obs + boost, 1)
    return {
        "nowcast": nowcast,
        "solar_noon_obs": solar_noon_obs,
        "boost": boost,
        "note": note,
        "suppressed": suppressed,
        "sky_cover": sky_cover,
        "wind_card": wind_card,
    }


def fetch_all(station="KOKC"):
    """
    Thin wrapper around _fetch_all_inner() that guarantees only one fetch can
    run for a given station at a time. bgloop's scheduled_fetch() and a
    manual /api/refresh both ultimately call this; without this lock they
    could run concurrently for the same station with no error and no
    distinguishing log output — each one's model requests just silently
    queue behind the other's through the shared _api_lock/_throttle(),
    making the whole thing take 2x+ as long for no visible reason (see the
    long comment near _fetch_locks above).
    """
    lock = _fetch_locks.get(station)
    if lock is None:
        # Unknown station (shouldn't happen — callers validate against
        # STATION_POOL — but don't silently skip a fetch over it).
        _fetch_all_inner(station)
        return
    acquired = lock.acquire(blocking=False)
    if not acquired:
        add_log(f"Fetch already in progress for {station} — skipping concurrent call", "warn", station)
        return
    try:
        _fetch_all_inner(station)
    finally:
        lock.release()


def _fetch_all_inner(station="KOKC"):
    st = get_state(station)
    if not API_KEY:
        add_log("No API key set", "err", station)
        return

    # --- BUG P2 FIX ---
    # The previous code had a "preview check" block here that called
    # _load_api_counter()/_get_period_key() and compared against a daily cap
    # *in addition to* the real check inside wethr_get() -> _check_and_increment().
    # That block didn't actually increment anything by itself, but every model
    # fetch below calls wethr_get() multiple times, and the duplicate accounting
    # logic made it easy to lose track of true usage and doubled the effective
    # cost bookkeeping (108 "slots" worth of checks for 54 real calls across
    # 18 models x 3 stations). _check_and_increment() inside wethr_get() is the
    # single source of truth for the (now uncapped, informational-only) usage
    # counter — nothing else should pre-check or duplicate it.
    add_log("Fetching data...", "info", station)
    errors = []

    # Observation
    try:
        obs = wethr_get(f"observations.php?station_code={station}&mode=latest")
        st["obs"] = obs
        add_log(
            f"Obs: {obs.get('temperature_display')}F "
            f"time={obs.get('observation_time')}",
            "ok",
            station
        )
        # Record solar noon obs if we're within ±30 min of solar noon
        try:
            lon_w = STATION_POOL.get(station, {}).get("lon_w")
            obs_temp = obs.get("temperature_display")
            if lon_w and obs_temp is not None:
                sn_utc = solar_noon_utc(lon_w)
                now_utc = datetime.utcnow()
                # Reset if stored obs is from a previous day
                sn_dt = st.get("solar_noon_dt")
                if sn_dt and sn_dt.date() < now_utc.date():
                    st["solar_noon_obs"] = None
                    st["solar_noon_dt"] = None
                diff_min = abs((now_utc - sn_utc).total_seconds() / 60)
                if diff_min <= 30:
                    existing = st.get("solar_noon_dt")
                    existing_diff = abs((existing - sn_utc).total_seconds() / 60) if existing else 9999
                    if existing is None or diff_min < existing_diff:
                        st["solar_noon_obs"] = float(obs_temp)
                        st["solar_noon_dt"] = now_utc
                        add_log(f"Solar noon obs: {obs_temp}F ({diff_min:.0f}min from solar noon)", "ok", station)
        except Exception as e:
            add_log(f"Solar noon record error (non-fatal): {e}", "warn", station)
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

    fetch_targets = active_models(station)
    if not fetch_targets:
        add_log("No accuracy data yet — using all known models", "warn", station)
        fetch_targets = ALL_KNOWN_MODELS

    utc_now = datetime.utcnow()
    tz_offset = STATION_POOL.get(station, {}).get("tz", -6)

    # --- FIX: forecasts.php requires start_valid_time / end_valid_time ---
    # The API rejected every forecasts.php call with HTTP 400:
    #   {"error":"Missing required parameters.",
    #    "details":["start_valid_time is required.","end_valid_time is required."]}
    # These were never being sent. We need both today's and tomorrow's data
    # (today_entries()/tomorrow_entries() below filter by local day boundaries
    # for this station), so request a window covering today's local midnight
    # through the end of tomorrow, expressed in UTC since that's what
    # station_day_bounds()/parse_vt() work in everywhere else in this file.
    today_start_utc, _ = station_day_bounds(station, 0)
    _, tomorrow_end_utc = station_day_bounds(station, 1)
    _VT_FMT = "%Y-%m-%d %H:%M"
    start_valid_time = today_start_utc.strftime(_VT_FMT)
    end_valid_time = tomorrow_end_utc.strftime(_VT_FMT)

    # Sequential model fetches with throttling (handled inside wethr_get)
    for model in fetch_targets:
        try:
            data = wethr_get(
                f"forecasts.php?location_name={station}&model={requests.utils.quote(model)}"
                f"&start_valid_time={requests.utils.quote(start_valid_time)}"
                f"&end_valid_time={requests.utils.quote(end_valid_time)}"
            )
            temps = data if isinstance(data, list) else data.get("forecasts", [])
            meta = {} if isinstance(data, list) else data
            if temps:
                # Log the raw keys of the first entry so we can see the API shape
                sample = temps[0]
                add_log(f"{model} sample keys: {list(sample.keys())}", "info", station)
                todays = today_entries(temps, station)
                if not todays:
                    add_log(f"{model}: no entries for today", "warn", station)
                    continue
                max_entry = max(todays, key=lambda x: get_temp(x) or 0)
                raw_temp = get_temp(max_entry)
                closest = min(todays, key=lambda x: abs(((parse_vt(x) or utc_now) - utc_now).total_seconds()))
                current_temp = get_temp(closest)
                run_raw = meta.get("run_time") or max_entry.get("run_time") or max_entry.get("run")
                run_fmt = fmt_run(run_raw)
                tomorrows = tomorrow_entries(temps, station)
                tmr_max = max(tomorrows, key=lambda x: get_temp(x) or 0) if tomorrows else None
                tmr_temp = get_temp(tmr_max) if tmr_max else None
                tmr_min = min(tomorrows, key=lambda x: get_temp(x) or 999) if tomorrows else None
                tmr_low = get_temp(tmr_min) if tmr_min else None
                tmr_low_time = None
                if tmr_min:
                    vt = parse_vt(tmr_min)
                    if vt:
                        local_vt = vt + timedelta(hours=tz_offset)
                        tmr_low_time = local_vt.strftime("%-I:%M%p").lower()

                # Conditions at peak temp hour
                wind_at_peak = _sf(max_entry.get("wind_speed_mph"))
                wind_dir = _sf(max_entry.get("wind_direction_deg"))
                cloud_at_peak = _sf(max_entry.get("cloud_cover"))
                dew_at_peak = _sf(max_entry.get("dew_point_f"))
                humid_at_peak = _sf(max_entry.get("relative_humidity"))

                # Day-wide: max gust and total precip
                max_gust = None
                total_precip = 0.0
                has_precip = False
                for entry in todays:
                    g = _sf(entry.get("wind_gusts_mph"))
                    if g is not None and (max_gust is None or g > max_gust):
                        max_gust = g
                    p = entry.get("precipitation_in")
                    if p is not None:
                        try:
                            total_precip += float(p)
                            has_precip = True
                        except:
                            pass

                st["forecasts"][model] = {
                    "high": raw_temp,
                    "current_fcst": current_temp,
                    "run": run_fmt,
                    "tmr_high": tmr_temp,
                    "tmr_low": tmr_low,
                    "tmr_low_time": tmr_low_time,
                    # Conditions
                    "wind_speed_mph": wind_at_peak,
                    "wind_dir_deg": wind_dir,
                    "wind_dir_card": deg_to_cardinal(wind_dir),
                    "wind_gust_mph": max_gust,
                    "cloud_cover": cloud_at_peak,
                    "dew_point_f": dew_at_peak,
                    "humidity_pct": humid_at_peak,
                    "precip_in": round(total_precip, 3) if has_precip else None,
                }
                if raw_temp is None:
                    add_log(f"{model}: WARNING raw_temp=None — check sample keys above.", "warn", station)
                else:
                    add_log(f"{model}: high={raw_temp} now={current_temp} run={run_fmt} ({len(todays)} entries)", "ok", station)
        except Exception as e:
            errors.append(f"{model}: {e}")
            add_log(f"{model} error: {str(e)}", "warn", station)
            print(f"FULL ERROR for {model}: {e}", flush=True)

    st["nws_versions"] = {}
    st["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st["errors"] = errors
    add_log(f"Done. {len(st['forecasts'])} models loaded.", "ok", station)

    # METAR — free, separate API, doesn't count against wethr budget
    try:
        metar = fetch_metar(station)
        st["metar"] = metar
        if metar:
            add_log(
                f"METAR: {metar['sky_label']} {' '.join(metar['sky_layers'])} "
                f"wind={metar.get('wind_dir_card','?')}@{metar.get('wind_speed_mph','?')}mph "
                f"vis={metar.get('visibility_sm','?')}SM",
                "ok", station
            )
    except Exception as e:
        add_log(f"METAR error (non-fatal): {e}", "warn", station)

    try:
        rows = build_snapshot_rows(station)
        save_pacing_snapshot(rows, station)
    except Exception as e:
        add_log(f"Snapshot error: {e}", "warn", station)

    try:
        now_local = station_local_now(station)
        if now_local.minute < 10 or (now_local.minute >= 30 and now_local.minute < 40):
            save_consensus_snapshot(station)
    except Exception as e:
        add_log(f"Consensus snapshot error: {e}", "warn", station)


_memory_snapshots = {}


def save_pacing_snapshot(rows, station="KOKC"):
    st = get_state(station)
    now = station_local_now(station)
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M")
    entry = {"time": time_str}
    for r in rows:
        if r.get("pace") is not None:
            entry[r["model"]] = r["pace"]
    key = f"{station}:{date_str}"
    if key not in _memory_snapshots:
        _memory_snapshots[key] = []
    _memory_snapshots[key].append(entry)
    avg = {}
    for r in rows:
        m = r["model"]
        vals = [s[m] for s in _memory_snapshots[key] if m in s]
        if vals:
            avg[m] = round(sum(vals) / len(vals), 2)
    st["today_avg_pace"] = avg
    try:
        ensure_data_dir()
        disk = load_json_file(f"{DATA_DIR}/pacing_{station}.json", {})
        if date_str not in disk:
            disk[date_str] = []
        disk[date_str].append(entry)
        keys = sorted(disk.keys())
        if len(keys) > 60:
            for k in keys[:-60]:
                del disk[k]
        save_json_file(f"{DATA_DIR}/pacing_{station}.json", disk)
    except Exception as e:
        add_log(f"Disk snapshot error (non-fatal): {e}", "warn", station)
    add_log(f"Snapshot: {len([r for r in rows if r.get('pace') is not None])} models", "info", station)


def rollup_daily_history(station="KOKC"):
    now = station_local_now(station)
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    pacing_file = f"{DATA_DIR}/pacing_{station}.json"
    history_file = f"{DATA_DIR}/history_{station}.json"
    snapshots = load_json_file(pacing_file, {})
    if yesterday not in snapshots or not snapshots[yesterday]:
        return
    history = load_json_file(history_file, {})
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
            daily_avg[m] = round(sum(vals) / len(vals), 2)
    history[yesterday] = {"avg_pace": daily_avg, "snapshot_count": len(day_snaps), "date": yesterday}
    save_json_file(history_file, history)
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
            pace = round(float(obs_temp) - float(current_fcst), 2) if obs_temp and current_fcst is not None else None
        except:
            pace = None
        rows.append({"model": model, "pace": pace})
    return rows


def scheduled_fetch():
    """
    Fetch each active station sequentially with a generous gap between them.
    Each station's model fetches are already serialized + throttled inside fetch_all.
    """
    active = get_active_stations()
    for i, station in enumerate(active):
        if i > 0:
            gap = 10 + random.uniform(2, 5)
            add_log(f"Waiting {gap:.0f}s before fetching next station", "info", active[i - 1])
            time.sleep(gap)
        try:
            fetch_all(station)
        except Exception as e:
            add_log(f"scheduled_fetch error: {e}", "err", station)


def save_consensus_snapshot(station="KOKC"):
    st = get_state(station)
    now = station_local_now(station)
    if now.hour < 6 or now.hour >= 22:
        return
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M")
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
        # Use the helper with default fallback
        run_data, _ = get_run_data(a, current_run)
        corr = run_data.get("correction") if run_data else None
        if corr in (None, ""):
            corr = a.get("correction")
        try:
            mae_val = run_data.get("mae") if run_data else None
            if not mae_val:
                mae_val = a.get("mae")
            mae = float(mae_val or 0)
            adj = round(float(raw) + float(corr), 1) if raw is not None and corr not in (None, "") else raw
            if mae > 0 and adj is not None:
                w = 1 / mae
                w_sum += adj * w
                w_total += w
        except:
            pass
        try:
            current_fcst = fcst.get("current_fcst")
            pace = round(float(obs_temp) - float(current_fcst), 2) if obs_temp and current_fcst is not None else None
            mae_val = run_data.get("mae") if run_data else None
            if not mae_val:
                mae_val = a.get("mae")
            mae = float(mae_val or 0)
            if mae > 0 and pace is not None:
                w = 1 / mae
                pw_sum += float(pace) * w
                pw_total += w
        except:
            pass
    consensus = round(w_sum / w_total, 1) if w_total > 0 else None
    cons_pace = round(pw_sum / pw_total, 2) if pw_total > 0 else None
    implied = round(consensus + cons_pace, 1) if consensus is not None and cons_pace is not None else None
    if consensus is None:
        return
    entry = {
        "time": time_str,
        "consensus": consensus,
        "implied": implied,
        "pace": cons_pace,
        "obs": float(obs_temp) if obs_temp else None,
        "date": date_str,
    }
    snaps = st["consensus_snapshots"]
    snaps = [s for s in snaps if s.get("date") == date_str]
    snaps.append(entry)
    st["consensus_snapshots"] = snaps[-48:]
    try:
        ensure_data_dir()
        path = f"{DATA_DIR}/consensus_{station}.json"
        disk = load_json_file(path, {})
        if date_str not in disk:
            disk[date_str] = []
        disk[date_str].append(entry)
        keys = sorted(disk.keys())
        if len(keys) > 90:
            for k in keys[:-90]:
                del disk[k]
        save_json_file(path, disk)
    except Exception as e:
        add_log(f"Consensus snapshot error: {e}", "warn", station)


# --- BUG P1 FIX (in-process) + CROSS-PROCESS FIX ---
# The lock above (_watchdog_lock) only prevents two threads in the SAME
# process from racing to spawn bgloop. It does nothing if the app is served
# by multiple OS processes (e.g. `gunicorn -w N` without --preload), because
# `with app.app_context(): start_background()` at module import time runs
# independently in EVERY worker process, each with its own fresh _started
# flag and its own _watchdog_lock. Each worker then spawns its own bgloop
# thread on its own schedule, and all of them hit the wethr API for the same
# stations — this is exactly what produced the duplicate KOKC/KPHL fetch
# batches ~15s apart in production logs.
#
# Fix: use an OS-level advisory file lock (flock) on a file in the shared
# /data volume. Only the ONE process that successfully grabs this exclusive,
# non-blocking lock is allowed to ever run background_loop(); every other
# worker's watchdog call becomes a no-op. The lock is held for the lifetime
# of that process (the fd is never closed), so if that worker dies/restarts,
# the lock is released automatically and another worker can pick it up.
_bgloop_lock_fd = None
_bgloop_owner_checked = False
_bgloop_owner_lock = threading.Lock()


def _is_bgloop_owner():
    """
    Returns True if THIS process holds the exclusive cross-process bgloop
    lock (acquiring it on first call if available). Returns False if some
    other process already owns it. Safe to call repeatedly/concurrently.
    """
    global _bgloop_lock_fd, _bgloop_owner_checked
    with _bgloop_owner_lock:
        if _bgloop_owner_checked:
            return _bgloop_lock_fd is not None
        _bgloop_owner_checked = True
        try:
            ensure_data_dir()
            fd = open(_BGLOOP_LOCKFILE, "w")
            fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fd.write(str(os.getpid()))
            fd.flush()
            _bgloop_lock_fd = fd
            print(f"BGLOOP LOCK ACQUIRED by pid {os.getpid()} — this process owns background_loop()", flush=True)
            return True
        except (IOError, OSError):
            print(f"BGLOOP LOCK held by another process — pid {os.getpid()} will not run background_loop()", flush=True)
            return False


def _ensure_background_thread_running():
    """
    Make sure exactly one 'bgloop' thread is alive, IN THIS PROCESS, and only
    if this process is the cross-process bgloop owner (see _is_bgloop_owner).
    Safe to call from many concurrent requests: the cheap check happens first
    with no locking, and only if that looks like bgloop is missing do we take
    the lock and check again before spawning. This collapses what used to be
    a classic check-then-act race (BUG P1) into a single winner, both within
    a process (_watchdog_lock) and across processes (_is_bgloop_owner).
    """
    if not _is_bgloop_owner():
        return
    for t in threading.enumerate():
        if t.name == "bgloop":
            return
    with _watchdog_lock:
        # Re-check inside the lock — another thread may have just won the race
        # and started bgloop while we were waiting for the lock.
        for t in threading.enumerate():
            if t.name == "bgloop":
                return
        print("WATCHDOG: restarting background thread", flush=True)
        t = threading.Thread(target=background_loop, daemon=True, name="bgloop")
        t.start()


def background_loop():
    print("BACKGROUND THREAD STARTING", flush=True)
    time.sleep(random.uniform(3, 8))
    while True:
        print("BACKGROUND LOOP RUNNING", flush=True)
        try:
            scheduled_fetch()
        except Exception as e:
            print(f"Loop error: {e}")
        try:
            for station in get_active_stations():
                now = station_local_now(station)
                if now.hour == 1:
                    rollup_daily_history(station)
        except Exception as e:
            print(f"Rollup error: {e}")
        time.sleep(REFRESH_SEC)


def _get_prev_days(n, station="KOKC"):
    history = load_json_file(f"{DATA_DIR}/history_{station}.json", {})
    keys = sorted(history.keys(), reverse=True)[:n]
    return [{"date": k, "avg_pace": history[k]["avg_pace"], "snapshot_count": history[k].get("snapshot_count", 0)} for k in keys]


@app.route("/api/state")
def api_state():
    station = request.args.get("station", "KOKC").upper()
    if station not in STATION_POOL:
        station = "KOKC"
    st = get_state(station)
    acc = st["accuracy"]
    models = active_models(station)
    rows = []
    for i, model in enumerate(models):
        a = acc.get(model, {})
        fcst = st["forecasts"].get(model, {})
        raw = fcst.get("high")
        current_run = fcst.get("run", "")
        # Use helper: exact run -> default fallback -> overall
        run_data, corr_source = get_run_data(a, current_run)
        corr = run_data.get("correction") if run_data else None
        display_mae = run_data.get("mae") if run_data else None
        if corr in (None, ""):
            corr = a.get("correction")
            if corr not in (None, ""):
                corr_source = "overall"
        if not display_mae:
            display_mae = a.get("mae")
        try:
            adj = round(float(raw) + float(corr), 1) if raw is not None and corr not in (None, "") else raw
        except:
            adj = None
        obs_temp = (st["obs"] or {}).get("temperature_display")
        current_fcst = fcst.get("current_fcst")
        try:
            pace = round(float(obs_temp) - float(current_fcst), 1) if obs_temp and current_fcst is not None else None
        except:
            pace = None
        tmr_raw = fcst.get("tmr_high")
        tmr_low = fcst.get("tmr_low")
        tmr_low_time = fcst.get("tmr_low_time")
        try:
            tmr_adj = round(float(tmr_raw) + float(corr), 1) if tmr_raw is not None and corr not in (None, "") else tmr_raw
        except:
            tmr_adj = tmr_raw
        try:
            tmr_low_adj = round(float(tmr_low) + float(corr), 1) if tmr_low is not None and corr not in (None, "") else tmr_low
        except:
            tmr_low_adj = tmr_low
        rows.append({
            "rank": i + 1, "model": model,
            "run": fcst.get("run", "—"),
            "raw_high": raw, "correction": corr,
            "corr_source": corr_source,  # "run", "default", or "overall"
            "adj_high": adj, "pace": pace,
            "tmr_high": tmr_raw, "tmr_adj": tmr_adj,
            "tmr_low": tmr_low, "tmr_low_adj": tmr_low_adj, "tmr_low_time": tmr_low_time,
            "mae": display_mae, "rmse": a.get("rmse"),
            "runs": a.get("runs", {}),
            # Conditions
            "wind_speed_mph": fcst.get("wind_speed_mph"),
            "wind_dir_deg": fcst.get("wind_dir_deg"),
            "wind_dir_card": fcst.get("wind_dir_card"),
            "wind_gust_mph": fcst.get("wind_gust_mph"),
            "cloud_cover": fcst.get("cloud_cover"),
            "dew_point_f": fcst.get("dew_point_f"),
            "humidity_pct": fcst.get("humidity_pct"),
            "precip_in": fcst.get("precip_in"),
        })

    w_sum, w_total = 0, 0
    for r in rows:
        try:
            mae = float(r["mae"])
            adj = r["adj_high"] if r["adj_high"] is not None else r["raw_high"]
            if mae > 0 and adj is not None:
                w = 1 / mae
                w_sum += adj * w
                w_total += w
        except:
            pass
    consensus = round(w_sum / w_total, 1) if w_total > 0 else None

    pw_sum, pw_total = 0, 0
    for r in rows:
        try:
            mae = float(r["mae"])
            pace = r["pace"]
            if mae > 0 and pace is not None:
                w = 1 / mae
                pw_sum += float(pace) * w
                pw_total += w
        except:
            pass
    consensus_pace = round(pw_sum / pw_total, 2) if pw_total > 0 else None

    tw_sum, tw_total = 0, 0
    for r in rows:
        try:
            mae = float(r["mae"])
            tadj = r["tmr_adj"] if r["tmr_adj"] is not None else r["tmr_high"]
            if mae > 0 and tadj is not None:
                w = 1 / mae
                tw_sum += tadj * w
                tw_total += w
        except:
            pass
    tmr_consensus = round(tw_sum / tw_total, 1) if tw_total > 0 else None

    # Conditions consensus (MAE-weighted) — wrapped so any failure can't break api/state
    cond_consensus = {}
    nowcast_data = None
    try:
        for field in ("wind_speed_mph", "wind_gust_mph", "cloud_cover", "dew_point_f", "humidity_pct", "precip_in"):
            ws, wt = 0, 0
            for r in rows:
                try:
                    mae = float(r["mae"])
                    v = r.get(field)
                    if mae > 0 and v is not None:
                        w = 1 / mae
                        ws += float(v) * w
                        wt += w
                except:
                    pass
            if wt > 0:
                cond_consensus[field] = round(ws / wt, 2)
        # Wind direction: vector average to handle 0/360 wrap
        sin_s, cos_s, wd_tot = 0, 0, 0
        for r in rows:
            try:
                mae = float(r["mae"])
                wd = r.get("wind_dir_deg")
                if mae > 0 and wd is not None:
                    w = 1 / mae
                    sin_s += w * math.sin(math.radians(float(wd)))
                    cos_s += w * math.cos(math.radians(float(wd)))
                    wd_tot += w
            except:
                pass
        if wd_tot > 0:
            avg_wd = math.degrees(math.atan2(sin_s / wd_tot, cos_s / wd_tot)) % 360
            cond_consensus["wind_dir_deg"] = round(avg_wd)
            cond_consensus["wind_dir_card"] = deg_to_cardinal(round(avg_wd))
        nowcast_data = compute_nowcast(station)
    except Exception as e:
        print(f"Conditions/nowcast compute error (non-fatal): {e}", flush=True)

    return jsonify({
        "station": station, "obs": st["obs"], "wethr_high": st["wethr_high"],
        "rows": rows, "consensus": consensus,
        "last_updated": st["last_updated"], "errors": st["errors"],
        "log": st["log"][:30], "models": active_models(station),
        "nws_versions": st["nws_versions"],
        "tmr_consensus": tmr_consensus,
        "consensus_pace": consensus_pace,
        "today_avg_pace": st["today_avg_pace"],
        "today_snapshot_count": len(load_json_file(f"{DATA_DIR}/pacing_{station}.json", {}).get(station_local_now(station).strftime("%Y-%m-%d"), [])),
        "prev_days": _get_prev_days(3, station),
        "metar": st.get("metar"),
        "conditions_consensus": cond_consensus,
        "nowcast": nowcast_data,
    })


@app.route("/api/history")
def api_history():
    station = request.args.get("station", "KOKC").upper()
    if station not in STATION_POOL:
        station = "KOKC"
    history = load_json_file(f"{DATA_DIR}/history_{station}.json", {})
    return jsonify(history)


@app.route("/api/accuracy", methods=["POST"])
def save_accuracy():
    station = request.args.get("station", "KOKC").upper()
    if station not in STATION_POOL:
        station = "KOKC"
    get_state(station)["accuracy"] = request.json or {}
    add_log("Accuracy data updated", "ok", station)
    save_json_file(f"{DATA_DIR}/accuracy_{station}.json", request.json or {})
    return jsonify({"ok": True})


@app.route("/api/consensus_snapshots")
def api_consensus_snapshots():
    station = request.args.get("station", "KOKC").upper()
    if station not in STATION_POOL:
        station = "KOKC"
    st = get_state(station)
    disk = load_json_file(f"{DATA_DIR}/consensus_{station}.json", {})
    return jsonify({
        "today": st.get("consensus_snapshots", []),
        "history": disk,
        "station": station,
    })


@app.route("/api/station_pool")
def api_station_pool():
    return jsonify({"pool": STATION_POOL, "active": get_active_stations()})


@app.route("/api/set_stations", methods=["POST"])
def api_set_stations():
    data = request.json or {}
    stations = data.get("stations", [])
    if set_active_stations(stations):
        return jsonify({"ok": True, "active": get_active_stations()})
    return jsonify({"ok": False, "error": "Need exactly 3 valid stations from the pool"}), 400


@app.route("/api/quota")
def api_quota():
    period = _get_period_key()
    data = _load_api_counter()
    count = data.get(period, 0)
    return jsonify({
        "period": period,
        "count": count,
        "cap": None,
        "remaining": None,
        "paused": False,
        "resets": "3:30pm EST daily (counter only, not enforced)",
    })


@app.route("/api/debug_threads")
def debug_threads():
    return jsonify({
        "started_flag": _started,
        "threads": [t.name for t in threading.enumerate()],
        "pid": os.getpid(),
    })


@app.route("/api/debug")
def api_debug():
    """Fetch one model raw and return the unprocessed API response for inspection.
    Uses the confirmed-correct forecasts.php call shape (location_name +
    start_valid_time + end_valid_time — discovered from wethr.net's own
    "Missing required parameters" error body). Pass ?raw=1 to also try the
    legacy guesswork variants for comparison if something still looks wrong."""
    station = request.args.get("station", "KOKC").upper()
    model = request.args.get("model", "HRRR")
    if station not in STATION_POOL:
        station = "KOKC"
    if not API_KEY:
        return jsonify({"error": "No API key set"})

    today_start_utc, _ = station_day_bounds(station, 0)
    _, tomorrow_end_utc = station_day_bounds(station, 1)
    _VT_FMT = "%Y-%m-%d %H:%M"
    start_valid_time = today_start_utc.strftime(_VT_FMT)
    end_valid_time = tomorrow_end_utc.strftime(_VT_FMT)

    correct_url = (
        f"forecasts.php?location_name={station}&model={requests.utils.quote(model)}"
        f"&start_valid_time={requests.utils.quote(start_valid_time)}"
        f"&end_valid_time={requests.utils.quote(end_valid_time)}"
    )
    attempts = [correct_url]
    if request.args.get("raw"):
        # Legacy guesswork shapes, kept only for comparison/troubleshooting —
        # none of these include the required time-range params either, so
        # expect all of them to 400 with the same "Missing required parameters"
        # body. They're here in case wethr.net ever changes the contract again.
        attempts += [
            f"forecasts.php?station_code={station}&model={requests.utils.quote(model)}&run=latest",
            f"forecasts.php?location_code={station}&model={requests.utils.quote(model)}&run=latest",
            f"forecasts.php?station={station}&model={requests.utils.quote(model)}&run=latest",
        ]

    results = {}
    for url in attempts:
        full_url = f"https://wethr.net/api/v2/{url}"
        try:
            r = requests.get(full_url, headers={"X-API-Key": API_KEY}, timeout=10)
            if r.status_code >= 400:
                results[url] = {
                    "status": "ERROR",
                    "http_status": r.status_code,
                    "response_body": (r.text or "")[:500],
                }
                continue
            data = r.json()
            temps = data if isinstance(data, list) else data.get("forecasts", [])
            sample = temps[:2] if temps else []
            results[url] = {
                "status": "OK",
                "http_status": r.status_code,
                "response_type": type(data).__name__,
                "top_level_keys": list(data.keys()) if isinstance(data, dict) else "list",
                "total_entries": len(temps),
                "sample_entries": sample,
            }
        except Exception as e:
            results[url] = {"status": "ERROR", "error": str(e)}
    return jsonify(results)


@app.route("/api/refresh", methods=["POST"])
def manual_refresh():
    station = request.args.get("station", "KOKC").upper()
    if station not in STATION_POOL:
        station = "KOKC"
    with _manual_refresh_lock:
        now = time.monotonic()
        elapsed = now - _last_manual_refresh.get(station, 0)
        if elapsed < MANUAL_REFRESH_COOLDOWN_SEC:
            remaining = round(MANUAL_REFRESH_COOLDOWN_SEC - elapsed)
            add_log(f"Manual refresh ignored (cooldown, {remaining}s left)", "warn", station)
            return jsonify({"ok": False, "cooldown": True, "remaining_sec": remaining})
        _last_manual_refresh[station] = now
    threading.Thread(target=fetch_all, args=(station,), daemon=True).start()
    return jsonify({"ok": True, "cooldown_sec": MANUAL_REFRESH_COOLDOWN_SEC})


# --- BUG P1 / P4 FIX ---
# The old watchdog ran @app.before_request unconditionally, on every single
# endpoint (including /api/quota, /api/debug_threads, etc.), recomputing
# threading.enumerate() each time and racily spawning duplicate bgloop
# threads under concurrent load. We now route all spawn requests through the
# single locked helper above so concurrent callers can't double-spawn, and we
# keep the watchdog itself cheap (no lock contention in the common case).
@app.before_request
def watchdog():
    _ensure_background_thread_running()


@app.route("/")
def index():
    return render_template_string(HTML)


HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Model Tracker</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600;700&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#080c10;--bg2:#0e1520;--bg3:#0b1118;--border:#1a2535;
  --text:#c9d4e0;--dim:#4a6080;--dimmer:#2a3a50;
  --blue:#38bdf8;--green:#4ade80;--yellow:#facc15;--red:#f87171;--purple:#c084fc;
  --orange:#fb923c;
}
body{background:var(--bg);color:var(--text);font-family:'IBM Plex Mono',monospace;font-size:13px}
.topbar{position:sticky;top:0;z-index:20;background:var(--bg3)}
header{background:var(--bg3);border-bottom:1px solid var(--border);padding:14px 20px;
  display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px}
h1{font-size:18px;color:#e8f0f8;letter-spacing:-.5px}
.sub{font-size:10px;color:var(--dim);letter-spacing:2px;text-transform:uppercase;margin-top:2px}
.hright{display:flex;align-items:center;gap:16px;flex-wrap:wrap}
.sp{width:1px;height:40px;background:var(--border)}
.stat-pill .lbl{font-size:9px;color:var(--dim);letter-spacing:2px;text-transform:uppercase}
.stat-pill .val{font-size:22px;font-weight:700;line-height:1.1}
.stat-pill .sub2{font-size:9px;color:var(--dimmer)}
nav{display:flex;gap:2px;background:var(--bg3);border-bottom:1px solid var(--border);padding:0 12px;overflow-x:auto}
nav button{background:none;border:none;border-bottom:2px solid transparent;color:var(--dim);
  padding:10px 16px;font-size:11px;letter-spacing:1.5px;text-transform:uppercase;
  cursor:pointer;font-family:inherit;transition:color .15s;white-space:nowrap}
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
.btn:disabled{opacity:.4;cursor:not-allowed}
.dot{width:7px;height:7px;border-radius:50%;display:inline-block;margin-right:5px}
.dot-green{background:var(--green);box-shadow:0 0 6px var(--green)}
.dot-red{background:var(--red);box-shadow:0 0 6px var(--red)}
.dot-yellow{background:var(--yellow);box-shadow:0 0 6px var(--yellow)}
.pbars{display:flex;flex-direction:column;gap:7px}
.prow{display:flex;align-items:center;gap:10px}
.plabel{width:80px;font-size:11px;color:#8aabcc}
.pbar{height:10px;border-radius:3px}
.logbox{background:#060a0e;border-radius:4px;padding:12px;max-height:400px;overflow-y:auto}
.pill-y{background:#facc1522;color:var(--yellow);border-radius:3px;padding:2px 7px;font-size:10px}
.pill-g{background:#4ade8022;color:var(--green);border-radius:3px;padding:2px 7px;font-size:10px}
.stn-btn{border-radius:4px;padding:5px 12px;font-size:11px;cursor:pointer;font-family:inherit}
.stn-btn.active{background:#1e40af;border:1px solid #3b82f6;color:#93c5fd}
.stn-btn.inactive{background:none;border:1px solid #334155;color:#64748b}
/* Default run column highlight */
.default-col{background:#fb923c0d !important}
th.default-col{color:var(--orange) !important}
</style>
</head>
<body>
<div class="topbar">
<header>
  <div>
    <h1 id="page-title">KOKC &middot; Model Tracker</h1>
    <div class="sub" id="page-sub">Oklahoma City Will Rogers World Airport</div>
  </div>
  <div class="hright">
    <div style="display:flex;gap:6px;align-items:center" id="station-btns"></div>
    <div class="sp"></div>
    <div style="text-align:right">
      <div style="display:flex;align-items:center;gap:6px;font-size:10px;color:var(--dim)">
        <span class="dot dot-yellow" id="sdot"></span><span id="stxt">Loading...</span>
      </div>
      <div style="font-size:10px;color:var(--dimmer);margin-top:3px">Next: <span id="cnt">5:00</span></div>
      <button class="btn" id="refresh-btn" style="margin-top:4px;padding:3px 10px;font-size:10px" onclick="manualRefresh()">Now</button>
    </div>
  </div>
</header>
<nav>
  <button class="active" onclick="showTab('dashboard',this)">&#128202; Dashboard</button>
  <button onclick="showTab('entry',this)">&#9728;&#65039; Morning Entry</button>
  <button onclick="showTab('runs',this)">&#128336; Run Accuracy</button>
  <button onclick="showTab('log',this)">&#128319; Log</button>
  <button onclick="showTab('history',this)">&#128196; History</button>
  <button onclick="showTab('snapshots',this)">&#128248; Snapshots</button>
</nav>
</div>
<main>
<!-- DASHBOARD -->
<div class="tab active" id="tab-dashboard">
  <div class="srow">
    <div class="sc"><div class="lbl">Current Temp</div><div class="v" id="s-obs" style="color:var(--yellow)">--</div></div>
    <div class="sc"><div class="lbl">Wethr High</div><div class="v" id="s-wh" style="color:var(--green)">--</div></div>
    <div class="sc"><div class="lbl">Consensus High</div><div class="v" id="s-con" style="color:var(--blue)">--</div></div>
    <div class="sc"><div class="lbl">Models Live</div><div class="v" id="s-mods" style="color:#a78bfa">--</div></div>
    <div class="sc"><div class="lbl">Tmr Consensus</div><div class="v" id="s-tmr" style="color:#a78bfa">--</div></div>
    <div class="sc" id="nowcast-sc" style="display:none"><div class="lbl">Nowcast High</div><div class="v" id="s-nowcast" style="color:var(--orange)">--</div><div class="s" id="s-nowcast-note"></div></div>
  </div>
  <div class="card">
    <div class="ctitle">
      Top 10 Models &mdash; Live Forecasts + Accuracy Adjustments
      <span class="pill-y" id="acc-badge" style="display:none">Enter accuracy in Morning Entry</span>
      <span class="pill-g" id="acc-loaded" style="display:none">Accuracy loaded</span>
    </div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr><th>#</th><th>Model</th><th>Run</th><th>Fcst High</th><th>Correction</th><th>Adj High</th><th>Pace</th><th>Tmr High</th><th>Tmr Adj</th><th>Tmr Low</th><th>Tmr Low Adj</th><th>Tmr Low Time</th><th>MAE</th><th>RMSE</th></tr></thead>
        <tbody id="main-tbody"></tbody>
      </table>
    </div>
  </div>
  <!-- CONDITIONS CARD -->
  <div class="card" id="conditions-card" style="display:none">
    <div class="ctitle">Conditions</div>
    <div style="font-size:10px;letter-spacing:1.5px;color:var(--dim);text-transform:uppercase;margin-bottom:8px">
      Surface Obs (METAR) &middot; <span id="metar-time" style="color:var(--dimmer)">--</span>
    </div>
    <div class="srow" style="margin-bottom:14px">
      <div class="sc"><div class="lbl">Sky Cover</div><div class="v" id="metar-sky" style="color:var(--blue)">--</div><div class="s" id="metar-ceil"></div></div>
      <div class="sc"><div class="lbl">Surface Wind</div><div class="v" id="metar-wind" style="color:var(--green)">--</div><div class="s" id="metar-gust"></div></div>
      <div class="sc"><div class="lbl">Visibility</div><div class="v" id="metar-vis" style="color:#a78bfa">--</div></div>
    </div>
    <div style="font-size:10px;letter-spacing:1.5px;color:var(--dim);text-transform:uppercase;margin-bottom:8px">
      Model Consensus &middot; At Peak Temp Hour
    </div>
    <div class="srow" style="margin-bottom:0">
      <div class="sc"><div class="lbl">Wind at Peak</div><div class="v" id="cond-wind" style="color:var(--blue)">--</div><div class="s" id="cond-wind-dir"></div></div>
      <div class="sc"><div class="lbl">Max Day Gust</div><div class="v" id="cond-gust" style="color:var(--yellow)">--</div></div>
      <div class="sc"><div class="lbl">Cloud Cover</div><div class="v" id="cond-cloud" style="color:var(--dim)">--</div></div>
      <div class="sc"><div class="lbl">Dew Point</div><div class="v" id="cond-dew" style="color:var(--green)">--</div><div class="s" id="cond-humid"></div></div>
      <div class="sc"><div class="lbl">Day Precip</div><div class="v" id="cond-precip" style="color:var(--dim)">--</div></div>
    </div>
  </div>
  <div class="card" id="pace-card" style="display:none">
    <div class="ctitle">Model Pacing vs Current Obs (<span id="pace-obs">--</span>F)</div>
    <div class="pbars" id="pbars"></div>
    <div style="font-size:10px;color:var(--dimmer);margin-top:10px">Pace = current obs minus model's current-hour forecast</div>
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
        <tbody id="avg-pace-tbody"><tr><td colspan="3" style="color:var(--dim)">Accumulating...</td></tr></tbody>
      </table>
    </div>
  </div>
  <div class="card" id="prev-days-card">
    <div class="ctitle">Previous 3 Days Average Pace</div>
    <div style="overflow-x:auto"><table><thead id="prev-days-thead"></thead><tbody id="prev-days-tbody"></tbody></table></div>
  </div>
  <div class="card" id="nws-card" style="display:none">
    <div class="ctitle">NWS Forecast Versions</div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr><th>Version</th><th>Fcst High</th><th>Adj High</th><th>Current Fcst</th><th>Pace</th></tr></thead>
        <tbody id="nws-tbody"></tbody>
      </table>
    </div>
  </div>
</div>
<!-- MORNING ENTRY -->
<div class="tab" id="tab-entry">
  <div class="card" style="border-color:#1e3a5f">
    <div class="ctitle">Active Stations</div>
    <p style="color:var(--dim);font-size:12px;line-height:1.7;margin-bottom:12px">Pick exactly 3 stations to track.</p>
    <div id="station-picker-grid" style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:14px"></div>
    <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">
      <button class="btn btn-green" onclick="saveActiveStations()">Save Active Stations</button>
      <span id="station-save-status" style="font-size:10px;color:var(--dim)"></span>
    </div>
  </div>
  <div class="card" style="border-color:#1e3a5f">
    <div class="ctitle">Fast Import &mdash; Paste JSON from Claude</div>
    <p style="color:var(--dim);font-size:12px;line-height:1.7;margin-bottom:12px">
      Each morning: screenshot accuracy tables, send to Claude, paste JSON here.
    </p>
    <textarea id="json-paste" placeholder="Paste JSON here..." style="width:100%;height:110px;background:var(--bg);border:1px solid #1e2e42;border-radius:4px;color:var(--text);padding:8px;font-family:inherit;font-size:11px"></textarea>
    <div style="display:flex;gap:10px;align-items:center;margin-top:10px;flex-wrap:wrap">
      <button class="btn" onclick="loadFromJSON()">Load JSON</button>
      <span style="font-size:10px;color:var(--dim)" id="json-status"></span>
    </div>
  </div>
  <!-- DEFAULT FALLBACK ENTRY -->
  <div class="card" style="border-color:#3a2a0a">
    <div class="ctitle" style="color:var(--orange)">&#9888; Default / Fallback Run Values</div>
    <p style="color:var(--dim);font-size:12px;line-height:1.7;margin-bottom:12px">
      Set a fallback MAE &amp; Correction per model. These apply automatically whenever a model
      has <em>no</em> run-specific entry &mdash; keeping it out of consensus rather than polluting it.
      <br><span style="color:var(--orange)">D</span> badge in the dashboard Correction column shows a default was used.
    </p>
    <div style="overflow-x:auto">
      <table>
        <thead>
          <tr>
            <th>Model</th>
            <th style="color:var(--orange)">Default MAE</th>
            <th style="color:var(--orange)">Default Correction</th>
            <th style="color:var(--dim);font-size:9px">Named Runs</th>
          </tr>
        </thead>
        <tbody id="default-tbody"></tbody>
      </table>
    </div>
    <div style="display:flex;gap:10px;align-items:center;margin-top:14px;flex-wrap:wrap">
      <button class="btn btn-green" onclick="saveDefaults()">Save Defaults</button>
      <button class="btn btn-red" onclick="clearDefaults()">Clear Defaults</button>
      <span style="font-size:10px;color:var(--dim)" id="default-status"></span>
    </div>
  </div>
  <details style="margin-bottom:16px">
    <summary style="cursor:pointer;color:var(--dim);font-size:11px;letter-spacing:1px;padding:8px 0">MANUAL ENTRY (advanced)</summary>
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
      </div>
      <div style="margin-top:14px;display:flex;gap:10px;align-items:center;flex-wrap:wrap">
        <button class="btn btn-green" onclick="saveAccuracy()">Save</button>
        <button class="btn btn-red" onclick="clearAccuracy()">Clear All</button>
        <span style="font-size:10px;color:var(--dim)" id="save-status"></span>
      </div>
    </div>
  </details>
  <div class="card" id="acc-preview" style="display:none">
    <div class="ctitle">Currently Loaded</div>
    <div style="overflow-x:auto"><table><thead><tr><th>Model</th><th>MAE</th><th>Correction</th><th>RMSE</th><th>Default MAE</th><th>Default Corr</th><th>Named Runs</th></tr></thead><tbody id="prev-tbody"></tbody></table></div>
    <div style="margin-top:10px;display:flex;gap:10px;align-items:center">
      <button class="btn btn-red" onclick="clearAccuracy()">Clear All</button>
      <span style="font-size:10px;color:var(--dim)" id="acc-loaded-time"></span>
    </div>
  </div>
</div>
<!-- RUN ACCURACY -->
<div class="tab" id="tab-runs">
  <div class="card">
    <div class="ctitle">Run-Specific Accuracy (including Default fallback)</div>
    <div style="overflow-x:auto"><table><thead><tr><th>Model</th><th class="default-col">DEFAULT</th><th>00Z</th><th>03Z</th><th>06Z</th><th>09Z</th><th>12Z</th><th>15Z</th><th>18Z</th><th>21Z</th></tr></thead><tbody id="runview-tbody"></tbody></table></div>
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
    <p style="color:var(--dim);font-size:11px;margin-bottom:12px">Average pace per model for previous days.</p>
    <div style="overflow-x:auto"><table><thead id="hist-thead"></thead><tbody id="hist-tbody"></tbody></table></div>
    <div style="font-size:10px;color:var(--dimmer);margin-top:10px" id="hist-count"></div>
  </div>
</div>
<!-- SNAPSHOTS TAB -->
<div class="tab" id="tab-snapshots">
  <div class="card">
    <div class="ctitle">Today&#39;s Consensus High Snapshots <span style="color:var(--dim);font-weight:400">&middot; every poll</span></div>
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
      <select id="snap-date-select" onchange="loadSnapshotDate()" style="background:var(--bg);border:1px solid var(--border);color:var(--text);padding:6px 10px;border-radius:4px;font-family:inherit">
        <option value="">Select date...</option>
      </select>
    </div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr><th>Time</th><th>Consensus High</th><th>Implied Adj High</th><th>Pace Adj</th><th>Obs</th></tr></thead>
        <tbody id="snap-hist-tbody"><tr><td colspan="5" style="color:var(--dim)">Select a date.</td></tr></tbody>
      </table>
    </div>
  </div>
</div>
</main>
<script>
var STATION_POOL_DATA = {}; // populated by loadStationPool()
var STATION_LIST = ["KOKC","KPHL","KDCA"]; // updated dynamically from server
var STATION_NAMES = {
  "KDCA": "Washington Reagan National Airport",
  "KOKC": "Oklahoma City Will Rogers World Airport",
  "KPHL": "Philadelphia International Airport",
  "KBOS": "Boston Logan International Airport",
  "KDEN": "Denver International Airport",
  "KHOU": "Houston Hobby Airport",
  "KLAS": "Las Vegas Harry Reid International",
  "KMDW": "Chicago Midway International Airport",
  "KMSP": "Minneapolis-St. Paul International",
  "KSAT": "San Antonio International Airport",
};
// Safe localStorage wrapper — Safari iOS throws SecurityError on localStorage
// when "Prevent Cross-Site Tracking" is enabled, killing the entire script.
var _ls = (function(){
  try { lsGet("__test__"); return localStorage; } catch(e){ return null; }
})();
function lsGet(k){ try{ return _ls ? _ls.getItem(k) : null; } catch(e){ return null; } }
function lsSet(k,v){ try{ if(_ls) _ls.setItem(k,v); } catch(e){} }
function lsRemove(k){ try{ if(_ls) _ls.removeItem(k); } catch(e){} }

var STATION = lsGet("active_station") || "KOKC";
// Will be corrected after loadStationPool() if no longer active
var MODELS = [];
var accData = {};

function safeLoadAcc(station){
  var raw = lsGet("acc_"+station);
  if(!raw) return {};
  try {
    var parsed = JSON.parse(raw);
    if(parsed && typeof parsed === "object" && !Array.isArray(parsed)) return parsed;
    return {};
  } catch(e){
    return {};
  }
}
accData = safeLoadAcc(STATION);
if(Object.keys(accData).length) MODELS = Object.keys(accData).filter(function(m){ return m !== "NWS"; });

var countdown = 300;
var countdownTimer;

// --- BUG J4: client-side cooldown tracking for manual refresh ---
var _refreshCooldownUntil = 0;
var _refreshCooldownTimer = null;

function buildStationButtons(){
  var container = document.getElementById("station-btns");
  if(!container) return;
  container.innerHTML = "";
  STATION_LIST.forEach(function(s){
    var btn = document.createElement("button");
    btn.id = "btn-"+s;
    btn.textContent = s;
    btn.className = "stn-btn " + (s === STATION ? "active" : "inactive");
    btn.onclick = function(){ switchStation(s); };
    container.appendChild(btn);
  });
}

function loadStationPool(){
  fetch("/api/station_pool").then(function(r){ return r.json(); }).then(function(data){
    STATION_POOL_DATA = data.pool || {};
    STATION_LIST = data.active || STATION_LIST;
    // If saved station is no longer active, switch to first active
    if(STATION_LIST.indexOf(STATION) === -1){
      STATION = STATION_LIST[0];
      lsSet("active_station", STATION);
      accData = safeLoadAcc(STATION);
      MODELS = Object.keys(accData).filter(function(m){ return m !== "NWS"; });
    }
    buildStationButtons();
    document.getElementById("page-sub").textContent = STATION_NAMES[STATION] || STATION;
    document.getElementById("page-title").textContent = STATION + " \u00b7 Model Tracker";
    // --- BUG J3 FIX ---
    // _selectedStations was captured from the hardcoded default STATION_LIST
    // at script-definition time, before this async call ever resolved. That
    // meant the picker in Morning Entry always showed the hardcoded defaults
    // until the user manually clicked something, even when the server's real
    // active stations were different. Now that STATION_LIST has just been
    // updated from the server response, sync the picker selection to match.
    _selectedStations = STATION_LIST.slice();
    renderStationPicker();
  }).catch(function(){ buildStationButtons(); });
}

// Build initial buttons from defaults while loadStationPool() is in flight
(function(){
  var container = document.getElementById("station-btns");
  if(!container) return;
  STATION_LIST.forEach(function(s){
    var btn = document.createElement("button");
    btn.id = "btn-"+s;
    btn.textContent = s;
    btn.className = "stn-btn " + (s === STATION ? "active" : "inactive");
    btn.onclick = function(){ switchStation(s); };
    container.appendChild(btn);
  });
})();

function updateStationButtons(){
  buildStationButtons();
}

function clearDisplay(){
  ["s-obs","s-wh","s-con","s-tmr"].forEach(function(id){
    var el = document.getElementById(id); if(el) el.textContent="--";
  });
  ["s-obs-t"].forEach(function(id){
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
  lsSet("active_station", s);
  clearDisplay();
  accData = safeLoadAcc(s);
  MODELS = Object.keys(accData).filter(function(m){ return m !== "NWS"; });
  updateStationButtons();
  document.getElementById("page-sub").textContent = STATION_NAMES[s] || s;
  document.getElementById("page-title").textContent = s + " \u00b7 Model Tracker";
  buildForms(); buildDefaultForm(); renderPreview(); poll();
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

// --- BUG J1 FIX ---
// Previously, nav buttons had BOTH an inline onclick="showTab(...)" handler
// AND a second addEventListener('click', ...) attached in a querySelectorAll
// loop at the bottom of the script. Every click fired showTab() twice, and
// for History/Snapshots that meant loadHistory()/loadSnapshots() (and their
// fetch() calls) fired twice per click too. Under normal conditions this was
// just wasteful; once the server was already saturated (BUG P1), those
// fetches could hang indefinitely, leaving the UI looking frozen. The fix:
// keep ONE source of truth for tab-switching (the inline onclick), and have
// showTab() itself trigger the tab-specific load function exactly once.
function showTab(id, btn){
  document.querySelectorAll(".tab").forEach(function(t){t.classList.remove("active");});
  document.querySelectorAll("nav button").forEach(function(b){b.classList.remove("active");});
  document.getElementById("tab-"+id).classList.add("active");
  btn.classList.add("active");
  if(id === "history") loadHistory();
  if(id === "snapshots") loadSnapshots();
}

function buildForms(){
  var ov = document.getElementById("ov-tbody");
  if(!ov) return;
  var mods = MODELS.length ? MODELS : ["HRRR","ARPEGE","NAM","UKMO","LAV-MOS","RAP","GEM-GDPS","NAM-MOS","NBM","NAM4KM","GFS","ICON","GFS-MOS","ECMWF-HRES","GEFS","JMA","RDPS","SREF"];
  ov.innerHTML = mods.map(function(m,i){
    var a = accData[m]||{};
    var bg = i%2?"background:#0a1018":"";
    return '<tr style="'+bg+'"><td style="color:#e8f0f8;font-weight:600">'+m+'</td>'
      +'<td><input type="number" step="0.1" placeholder="0.0" id="ov-mae-'+m+'" value="'+(a.mae!=null?a.mae:"")+'"></td>'
      +'<td><input type="number" step="0.1" placeholder="0.0" id="ov-corr-'+m+'" value="'+(a.correction!=null?a.correction:"")+'"></td>'
      +'<td><input type="number" step="0.1" placeholder="0.0" id="ov-rmse-'+m+'" value="'+(a.rmse!=null?a.rmse:"")+'"></td></tr>';
  }).join("");
  var rb = document.getElementById("run-tbody");
  rb.innerHTML = mods.map(function(m,i){
    var a = accData[m]||{};
    var bg = i%2?"background:#0a1018":"";
    var cells = MANUAL_RUNS.map(function(r){
      var rd = (a.runs||{})[r]||{};
      return '<td style="padding:5px 6px"><div style="display:flex;flex-direction:column;gap:3px">'
        +'<input type="number" step="0.1" placeholder="MAE" style="width:56px;font-size:11px" id="rm-mae-'+m+'-'+r+'" value="'+(rd.mae!=null?rd.mae:"")+'">'
        +'<input type="number" step="0.1" placeholder="Corr" style="width:56px;font-size:11px" id="rm-corr-'+m+'-'+r+'" value="'+(rd.correction!=null?rd.correction:"")+'">'
        +'</div></td>';
    }).join("");
    return '<tr style="'+bg+'"><td style="color:#8aabcc;font-weight:600">'+m+'</td>'+cells+'</tr>';
  }).join("");
}

// --- DEFAULT FALLBACK FORM ---
function buildDefaultForm(){
  var mods = MODELS.length ? MODELS : ["HRRR","ARPEGE","NAM","UKMO","LAV-MOS","RAP","GEM-GDPS","NAM-MOS","NBM","NAM4KM","GFS","ICON","GFS-MOS","ECMWF-HRES","GEFS","JMA","RDPS","SREF"];
  var tbody = document.getElementById("default-tbody");
  if(!tbody) return;
  tbody.innerHTML = mods.map(function(m,i){
    var a = accData[m]||{};
    var rd = (a.runs||{})["default"]||{};
    var bg = i%2?"background:#0a1018":"";
    var namedRuns = Object.keys(a.runs||{}).filter(function(r){ return r!=="default"; }).join(", ") || "—";
    return '<tr style="'+bg+'">'
      +'<td style="color:#e8f0f8;font-weight:600">'+m+'</td>'
      +'<td class="default-col"><input type="number" step="0.1" placeholder="e.g. 1.5" style="width:80px" id="def-mae-'+m+'" value="'+(rd.mae!=null?rd.mae:"")+'"></td>'
      +'<td class="default-col"><input type="number" step="0.1" placeholder="e.g. +0.5" style="width:80px" id="def-corr-'+m+'" value="'+(rd.correction!=null?rd.correction:"")+'"></td>'
      +'<td style="color:var(--dim);font-size:11px">'+namedRuns+'</td>'
      +'</tr>';
  }).join("");
}

function saveDefaults(){
  var mods = MODELS.length ? MODELS : [];
  var status = document.getElementById("default-status");
  mods.forEach(function(m){
    if(!accData[m]) accData[m] = {};
    if(!accData[m].runs) accData[m].runs = {};
    var maeEl = document.getElementById("def-mae-"+m);
    var corrEl = document.getElementById("def-corr-"+m);
    var mae = maeEl ? maeEl.value : "";
    var corr = corrEl ? corrEl.value : "";
    if(mae || corr){
      accData[m].runs["default"] = { mae: mae, correction: corr };
    } else {
      delete accData[m].runs["default"];
    }
  });
  lsSet("acc_"+STATION, JSON.stringify(accData));
  fetch("/api/accuracy?station="+STATION,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(accData)})
    .then(function(r){ return r.json(); })
    .then(function(){
      status.style.color="var(--green)";
      status.textContent = "Defaults saved at "+new Date().toLocaleTimeString();
      renderPreview();
    }).catch(function(e){
      lsSet("acc_"+STATION, JSON.stringify(accData));
      status.style.color="var(--yellow)";
      status.textContent = "Saved locally (server: "+e.message+")";
      renderPreview();
    });
}

function clearDefaults(){
  if(!confirm("Clear all default fallback values?")) return;
  var mods = MODELS.length ? MODELS : [];
  mods.forEach(function(m){
    if(accData[m] && accData[m].runs) delete accData[m].runs["default"];
  });
  lsSet("acc_"+STATION, JSON.stringify(accData));
  fetch("/api/accuracy?station="+STATION,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(accData)});
  buildDefaultForm();
  document.getElementById("default-status").textContent = "Defaults cleared";
  renderPreview();
}

function loadFromJSON(){
  var raw = document.getElementById("json-paste").value.trim();
  var status = document.getElementById("json-status");
  if(!raw){ status.style.color="var(--red)"; status.textContent="Nothing to paste."; return; }
  try {
    var parsed = JSON.parse(raw);
    if(!parsed || typeof parsed !== "object" || Array.isArray(parsed)){
      status.style.color="var(--red)"; status.textContent="JSON must be an object of models."; return;
    }
    var keys = Object.keys(parsed);
    if(!keys.length){ status.style.color="var(--red)"; status.textContent="No models found."; return; }
    // Preserve existing default run values when importing new JSON
    keys.forEach(function(m){
      if(accData[m] && accData[m].runs && accData[m].runs["default"]){
        if(!parsed[m].runs) parsed[m].runs = {};
        if(!parsed[m].runs["default"]){
          parsed[m].runs["default"] = accData[m].runs["default"];
        }
      }
    });
    accData = parsed;
    MODELS = keys.filter(function(m){ return m !== "NWS"; });
    lsSet("acc_"+STATION, JSON.stringify(parsed));
    lsSet("acc_"+STATION+"_time", new Date().toLocaleString());
    fetch("/api/accuracy?station="+STATION,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(parsed)})
      .then(function(r){
        if(!r.ok) throw new Error("HTTP "+r.status);
        status.style.color="var(--green)";
        status.textContent = "Loaded "+keys.length+" models at "+new Date().toLocaleTimeString();
        document.getElementById("json-paste").value="";
        buildForms(); buildDefaultForm(); renderPreview(); poll();
      }).catch(function(e){
        status.style.color="var(--yellow)";
        status.textContent = "Saved locally (server: "+e.message+"). Will sync on next refresh.";
        buildForms(); buildDefaultForm(); renderPreview();
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
    // Preserve defaults when saving manual entries
    if(accData[m] && accData[m].runs && accData[m].runs["default"]){
      data[m].runs["default"] = accData[m].runs["default"];
    }
  });
  accData = data;
  lsSet("acc_"+STATION, JSON.stringify(data));
  fetch("/api/accuracy?station="+STATION,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(data)})
    .then(function(){ document.getElementById("save-status").textContent="Saved "+new Date().toLocaleTimeString(); });
}

function clearAccuracy(){
  if(!confirm("Clear all accuracy data?")) return;
  accData = {}; MODELS = [];
  lsRemove("acc_"+STATION); lsRemove("acc_"+STATION+"_time");
  fetch("/api/accuracy?station="+STATION,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({})});
  buildForms(); buildDefaultForm(); renderPreview();
  document.getElementById("save-status").textContent="Cleared";
}

function renderPreview(){
  var hasAny = Object.keys(accData).some(function(m){ return accData[m] && accData[m].mae; });
  var el = document.getElementById("acc-preview");
  document.getElementById("acc-badge").style.display = hasAny ? "none" : "inline";
  document.getElementById("acc-loaded").style.display = hasAny ? "inline" : "none";
  if(!hasAny){ el.style.display="none"; return; }
  el.style.display="block";
  var t = lsGet("acc_"+STATION+"_time");
  if(t) document.getElementById("acc-loaded-time").textContent="Loaded: "+t;
  var mods = Object.keys(accData);
  document.getElementById("prev-tbody").innerHTML = mods.map(function(m,i){
    var a = accData[m]||{};
    var defRd = (a.runs||{})["default"]||{};
    var namedRuns = Object.keys(a.runs||{}).filter(function(r){ return r!=="default"; }).join(", ") || "—";
    var bg = i%2?"background:#0a1018":"";
    return '<tr style="'+bg+'">'
      +'<td style="color:#e8f0f8;font-weight:600">'+m+'</td>'
      +'<td style="color:'+maeColor(a.mae)+'">'+(a.mae?fmt1(a.mae)+"F":"--")+'</td>'
      +'<td style="color:'+corrColor(a.correction)+'">'+(a.correction!=null&&a.correction!==""?fmtC(a.correction):"--")+'</td>'
      +'<td style="color:var(--dim)">'+(a.rmse?fmt1(a.rmse)+"F":"--")+'</td>'
      +'<td style="color:'+(defRd.mae?maeColor(defRd.mae):"var(--dimmer)")+'">'+(defRd.mae?fmt1(defRd.mae)+"F":"--")+'</td>'
      +'<td style="color:'+(defRd.correction!=null&&defRd.correction!==""?corrColor(defRd.correction):"var(--dimmer)")+'">'+(defRd.correction!=null&&defRd.correction!==""?fmtC(defRd.correction):"--")+'</td>'
      +'<td style="color:var(--dim);font-size:11px">'+namedRuns+'</td></tr>';
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
    document.getElementById("s-obs").textContent = t+"F";
    document.getElementById("pace-obs").textContent = t;
  }
  if(wh){ document.getElementById("s-wh").textContent=wh.wethr_high+"F"; }
  if(con){ document.getElementById("s-con").textContent=con+"F"; }
  var tmrCon = data.tmr_consensus;
  if(tmrCon){
    document.getElementById("s-tmr").textContent=tmrCon+"F";
  }
  // Solar-adjusted nowcast high
  var nc = data.nowcast;
  var ncSc = document.getElementById("nowcast-sc");
  if(nc && nc.nowcast != null){
    document.getElementById("s-nowcast").textContent = nc.nowcast + "F";
    document.getElementById("s-nowcast").style.color = nc.suppressed ? "var(--red)" : "var(--orange)";
    document.getElementById("s-nowcast-note").textContent =
      nc.solar_noon_obs + "F + " + nc.boost + "F \u00b7 " + nc.note;
    if(ncSc) ncSc.style.display = "";
  } else {
    if(ncSc) ncSc.style.display = "none";
  }
  document.getElementById("s-mods").textContent = rows.filter(function(r){ return r.raw_high!=null; }).length+"/"+rows.length;
  document.getElementById("main-tbody").innerHTML = rows.map(function(r,i){
    var bg = i%2?"background:#0a1018":"";
    // Correction source badge
    var corrBadge = "";
    if(r.corr_source === "run") corrBadge = ' <span style="font-size:9px;color:#38bdf8" title="Run-specific">R</span>';
    else if(r.corr_source === "default") corrBadge = ' <span style="font-size:9px;color:var(--orange)" title="Default fallback">D</span>';
    return '<tr style="'+bg+'">'
      +'<td style="color:var(--dim)">#'+r.rank+'</td>'
      +'<td style="color:#e8f0f8;font-weight:600">'+r.model+'</td>'
      +'<td style="color:var(--dim);font-size:11px">'+(r.run||"--")+'</td>'
      +'<td style="color:var(--yellow)">'+(r.raw_high!=null?r.raw_high+"F":"--")+'</td>'
      +'<td style="color:'+corrColor(r.correction)+'">'+(r.correction!=null&&r.correction!==""?fmtC(r.correction):"--")+corrBadge+'</td>'
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
  var paceRows = rows.filter(function(r){ return r.pace!=null; });
  if(paceRows.length && obs){
    document.getElementById("pace-card").style.display="block";
    document.getElementById("pbars").innerHTML = paceRows.map(function(r){
      var p=Number(r.pace); var w=Math.min(Math.abs(p)*14,140);
      var col=p>=0?"var(--green)":"var(--red)";
      return '<div class="prow"><div class="plabel">'+r.model+'</div>'
        +'<div style="width:160px"><div class="pbar" style="width:'+w+'px;background:'+col+'"></div></div>'
        +'<span style="font-size:11px;color:'+paceColor(r.pace)+';font-weight:600">'+(p>=0?"+":"")+p+"F</span></div>";
    }).join("");
  } else {
    document.getElementById("pace-card").style.display="none";
  }
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
      return '<tr style="'+bg+'"><td style="color:'+vc+';font-weight:600">'+vl+'</td>'
        +'<td style="color:var(--yellow)">'+(v.high!=null?v.high+"F":"--")+'</td>'
        +'<td style="color:var(--green)">'+(adj?adj+"F":"--")+'</td>'
        +'<td style="color:#94a3b8">'+(v.current_fcst!=null?v.current_fcst+"F":"--")+'</td>'
        +'<td style="color:'+pc+'">'+ps+'</td></tr>';
    }).join("");
  } else {
    nwsCard.style.display="none";
  }
  // Run Accuracy tab — includes DEFAULT column
  document.getElementById("runview-tbody").innerHTML = rows.map(function(r,i){
    var bg = i%2?"background:#0a1018":"";
    var defRd = (r.runs||{})["default"]||{};
    var defCell = (defRd.mae||defRd.correction)
      ?'<td class="default-col" style="text-align:center"><div style="line-height:1.8">'
        +(defRd.mae?'<div style="color:'+maeColor(defRd.mae)+'">'+fmt1(defRd.mae)+'F</div>':'')
        +(defRd.correction!=null&&defRd.correction!==""?'<div style="color:'+corrColor(defRd.correction)+'">'+fmtC(defRd.correction)+'</div>':'')
        +'</div></td>'
      :'<td class="default-col" style="text-align:center"><span style="color:#1e2e42">--</span></td>';
    var cells = MANUAL_RUNS.map(function(run){
      var rd = (r.runs||{})[run]||{};
      var has = rd.mae||rd.correction;
      return '<td style="text-align:center">'+(has
        ?'<div style="line-height:1.8">'+(rd.mae?'<div style="color:'+maeColor(rd.mae)+'">'+fmt1(rd.mae)+'F</div>':'')+
          (rd.correction!=null&&rd.correction!==""?'<div style="color:'+corrColor(rd.correction)+'">'+fmtC(rd.correction)+'</div>':'')+'</div>'
        :'<span style="color:#1e2e42">--</span>')+'</td>';
    }).join("");
    return '<tr style="'+bg+'"><td style="color:#e8f0f8;font-weight:600">'+r.model+'</td>'+defCell+cells+'</tr>';
  }).join("");
  document.getElementById("run-cards").innerHTML = rows.map(function(r){
    var runKey = r.run ? r.run.replace(/[^0-9]/g,"").slice(0,2)+"Z" : "";
    var rd = (r.runs||{})[runKey]||{};
    var hasC = rd.correction!=null&&rd.correction!=="";
    var usingDefault = !hasC && (r.runs||{})["default"] && ((r.runs||{})["default"].correction != null && (r.runs||{})["default"].correction !== "");
    var defRd = (r.runs||{})["default"]||{};
    return '<div style="background:#0b1520;border:1px solid '+(usingDefault?"var(--orange)":"var(--border)")+';border-radius:6px;padding:8px 12px;min-width:110px">'
      +'<div style="font-size:11px;color:#8aabcc;font-weight:600">'+r.model+'</div>'
      +'<div style="font-size:13px;color:var(--blue);margin-top:2px">'+(r.run||"--")+'</div>'
      +(hasC?'<div style="font-size:11px;color:'+corrColor(rd.correction)+';margin-top:2px">Corr: '+fmtC(rd.correction)+'</div>'
        :usingDefault?'<div style="font-size:11px;color:var(--orange);margin-top:2px">Default: '+fmtC(defRd.correction)+'</div>'
        :'<div style="font-size:10px;color:#2a4060;margin-top:2px">No run corr</div>')
      +'</div>';
  }).join("");
  if(data.log&&data.log.length){
    document.getElementById("logbox").innerHTML = data.log.map(function(e){
      var col = e.level==="ok"?"var(--green)":e.level==="err"?"var(--red)":e.level==="warn"?"var(--yellow)":"var(--dim)";
      return '<div style="margin-bottom:5px"><span style="color:var(--dimmer)">['+e.t+']</span> <span style="color:'+col+'">'+e.msg+'</span></div>';
    }).join("");
  }
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
  // Conditions card
  var metar = data.metar;
  var conds = data.conditions_consensus || {};
  var condCard = document.getElementById("conditions-card");
  if(metar || Object.keys(conds).length){
    condCard.style.display = "block";
    if(metar){
      document.getElementById("metar-time").textContent =
        metar.obs_time_utc ? metar.obs_time_utc.slice(11,16)+"Z" : "--";
      document.getElementById("metar-sky").textContent = metar.sky_label || metar.sky_cover || "--";
      var layers = (metar.sky_layers||[]).join(" ") || "--";
      var ceilStr = metar.ceiling_ft
        ? "ceiling " + metar.ceiling_ft.toLocaleString() + "ft"
        : (["SKC","CLR","CAVOK","FEW","SCT"].indexOf(metar.sky_cover||"") >= 0 ? "no ceiling" : "--");
      document.getElementById("metar-ceil").textContent = layers + " \u00b7 " + ceilStr;
      var mw = "--";
      if(metar.wind_speed_mph != null){
        mw = (metar.wind_dir_card ? metar.wind_dir_card+" " : "") + metar.wind_speed_mph + "mph";
      }
      document.getElementById("metar-wind").textContent = mw;
      document.getElementById("metar-gust").textContent =
        metar.wind_gust_mph ? "gust " + metar.wind_gust_mph + "mph" : "no gusts reported";
      document.getElementById("metar-vis").textContent =
        metar.visibility_sm != null ? metar.visibility_sm + " SM" : "--";
    }
    if(conds.wind_speed_mph != null){
      document.getElementById("cond-wind").textContent = conds.wind_speed_mph.toFixed(1) + "mph";
      // --- BUG J5 FIX ---
      // Previously this always rendered "from " + wind_dir_card + " (" + deg + "°)"
      // even when wind_dir_card was null (only wind_dir_deg present), producing
      // the literal string "from null (270°)". Now we only render the cardinal
      // direction text when we actually have one.
      if(conds.wind_dir_card){
        document.getElementById("cond-wind-dir").textContent =
          "from " + conds.wind_dir_card + (conds.wind_dir_deg != null ? " (" + Math.round(conds.wind_dir_deg) + "\u00b0)" : "");
      } else if(conds.wind_dir_deg != null){
        document.getElementById("cond-wind-dir").textContent = Math.round(conds.wind_dir_deg) + "\u00b0";
      } else {
        document.getElementById("cond-wind-dir").textContent = "";
      }
    }
    if(conds.wind_gust_mph != null)
      document.getElementById("cond-gust").textContent = conds.wind_gust_mph.toFixed(1) + "mph";
    if(conds.cloud_cover != null){
      document.getElementById("cond-cloud").textContent = Math.round(conds.cloud_cover) + "%";
      document.getElementById("cond-cloud").style.color =
        conds.cloud_cover < 25 ? "var(--yellow)" : conds.cloud_cover < 60 ? "var(--blue)" : "var(--dim)";
    }
    if(conds.dew_point_f != null){
      document.getElementById("cond-dew").textContent = conds.dew_point_f.toFixed(1) + "F";
      document.getElementById("cond-humid").textContent =
        conds.humidity_pct != null ? Math.round(conds.humidity_pct) + "% humidity" : "--";
    }
    if(conds.precip_in != null){
      var precip = conds.precip_in;
      document.getElementById("cond-precip").textContent = precip > 0 ? precip.toFixed(2) + '"' : "0\"";
      document.getElementById("cond-precip").style.color = precip > 0.1 ? "var(--blue)" : "var(--dim)";
    }
  } else {
    condCard.style.display = "none";
  }
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
    document.getElementById("avg-pace-tbody").innerHTML = '<tr><td colspan="3" style="color:var(--dim)">Accumulating...</td></tr>';
  }
  var prevDays = data.prev_days || [];
  if(prevDays.length){
    var allModels = [];
    prevDays.forEach(function(d){ Object.keys(d.avg_pace).forEach(function(m){ if(allModels.indexOf(m)<0) allModels.push(m); }); });
    document.getElementById("prev-days-thead").innerHTML = '<tr><th>Model</th>'+prevDays.map(function(d){ return '<th>'+d.date+'</th>'; }).join("")+'</tr>';
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
    document.getElementById("prev-days-tbody").innerHTML = '<tr><td style="color:var(--dim)">No history yet.</td></tr>';
  }
  document.getElementById("sdot").className = "dot "+(data.errors&&data.errors.length?"dot-yellow":"dot-green");
  document.getElementById("stxt").textContent = data.last_updated?"Updated "+data.last_updated.slice(11):"No data";
}

function poll(){
  // BUG FIX: removed fire-and-forget POST /api/accuracy that fired on every
  // poll. That accuracy data never changes between explicit user saves, so
  // resending it every 60s was pure waste. Worse: in single-threaded Werkzeug
  // the POST and the state GET arrived simultaneously; Werkzeug served the POST
  // first, and burst-firing (focus + visibilitychange + initial load at once)
  // queued several POSTs ahead of each GET. If save_json_file had any latency
  // those POSTs occupied all 6 Chrome per-origin connection slots and the state
  // GET never got served - UI frozen, zero errors in the Flask log.
  // Accuracy is now only POSTed when the user explicitly saves (saveAccuracy,
  // saveDefaults, loadFromJSON, clearAccuracy) - the correct set of call sites.
  fetch("/api/state?station="+STATION).then(function(r){ return r.json(); }).then(render).catch(function(e){ console.error("Poll error",e); });
}

// --- BUG J4 FIX ---
// The server already enforces a cooldown and returns {ok:false, cooldown:true,
// remaining_sec:N} when a refresh is rejected, but the client never tracked
// or displayed this, so clicking "Now" repeatedly during the cooldown window
// silently did nothing with no feedback. We now disable the button and show
// a live countdown for the remaining cooldown, using the cooldown info from
// either a successful response (which includes cooldown_sec) or a rejected
// one (which includes remaining_sec).
function startRefreshCooldownDisplay(remainingSec){
  clearInterval(_refreshCooldownTimer);
  _refreshCooldownUntil = Date.now() + remainingSec*1000;
  var btn = document.getElementById("refresh-btn");
  function tick(){
    var left = Math.ceil((_refreshCooldownUntil - Date.now())/1000);
    if(left <= 0){
      clearInterval(_refreshCooldownTimer);
      btn.disabled = false;
      btn.textContent = "Now";
      return;
    }
    btn.disabled = true;
    btn.textContent = "Wait " + left + "s";
  }
  tick();
  _refreshCooldownTimer = setInterval(tick, 1000);
}

function manualRefresh(){
  fetch("/api/refresh?station="+STATION,{method:"POST"})
    .then(function(r){ return r.json(); })
    .then(function(data){
      if(data && data.cooldown){
        startRefreshCooldownDisplay(data.remaining_sec || MANUAL_REFRESH_COOLDOWN_SEC_CLIENT);
        return;
      }
      if(data && data.cooldown_sec){
        startRefreshCooldownDisplay(data.cooldown_sec);
      }
      countdown=300;
      document.getElementById("stxt").textContent="Fetching...";
      setTimeout(poll,5000);
      setTimeout(poll,20000);
      setTimeout(poll,40000);
    }).catch(function(){
      // Fall back to old behavior if the request itself failed
      countdown=300;
      setTimeout(poll,5000);
    });
}
var MANUAL_REFRESH_COOLDOWN_SEC_CLIENT = 120; // mirrors server MANUAL_REFRESH_COOLDOWN_SEC

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

// --- Station picker ---
var _selectedStations = STATION_LIST.slice();
function renderStationPicker(){
  var grid = document.getElementById("station-picker-grid");
  if(!grid) return;
  var allCodes = Object.keys(STATION_POOL_DATA).length ? Object.keys(STATION_POOL_DATA)
    : ["KDCA","KOKC","KPHL","KBOS","KDEN","KHOU","KLAS","KMDW","KMSP","KSAT"];
  grid.innerHTML = allCodes.map(function(code){
    var active = _selectedStations.indexOf(code) >= 0;
    return '<button onclick="togglePoolStation(\''+code+'\')" id="pool-btn-'+code+'" '
      +'style="background:'+(active?"#1e40af":"none")+';border:1px solid '+(active?"#3b82f6":"#334155")+';'
      +'color:'+(active?"#93c5fd":"#64748b")+';border-radius:4px;padding:6px 14px;font-size:11px;'
      +'cursor:pointer;font-family:inherit;letter-spacing:1px">'+code+'</button>';
  }).join("");
}
function togglePoolStation(code){
  var idx = _selectedStations.indexOf(code);
  if(idx >= 0){
    if(_selectedStations.length > 1) _selectedStations.splice(idx, 1);
  } else {
    if(_selectedStations.length < 3) _selectedStations.push(code);
  }
  renderStationPicker();
  var status = document.getElementById("station-save-status");
  if(status) status.textContent = _selectedStations.length === 3 ? "" : "Select " + (3 - _selectedStations.length) + " more";
}
function saveActiveStations(){
  var status = document.getElementById("station-save-status");
  if(_selectedStations.length !== 3){
    status.style.color = "var(--red)";
    status.textContent = "Select exactly 3 stations.";
    return;
  }
  fetch("/api/set_stations",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({stations:_selectedStations})})
    .then(function(r){ return r.json(); })
    .then(function(data){
      if(data.ok){
        STATION_LIST = data.active;
        status.style.color = "var(--green)";
        status.textContent = "Saved: " + data.active.join(", ");
        buildStationButtons();
        if(data.active.indexOf(STATION) < 0){
          switchStation(data.active[0]);
        }
      } else {
        status.style.color = "var(--red)";
        status.textContent = data.error || "Error saving.";
      }
    });
}

document.getElementById("page-title").textContent = STATION + " \u00b7 Model Tracker";
document.getElementById("page-sub").textContent = STATION_NAMES[STATION] || STATION;
buildForms(); buildDefaultForm(); renderPreview(); poll(); startCountdown();
loadStationPool(); // fetch active stations + update header buttons + render picker

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
          return '<tr style="'+bg+'"><td style="color:var(--dim)">'+s.time+'</td>'
            +'<td style="color:var(--blue);font-weight:600">'+(s.consensus!=null?s.consensus+"F":"--")+'</td>'
            +'<td style="color:var(--green);font-weight:600">'+(s.implied!=null?s.implied+"F":"--")+'</td>'
            +'<td style="color:'+pc+'">'+paceStr+'</td>'
            +'<td style="color:var(--yellow)">'+(s.obs!=null?s.obs+"F":"--")+'</td></tr>';
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
    return '<tr style="'+bg+'"><td style="color:var(--dim)">'+s.time+'</td>'
      +'<td style="color:var(--blue);font-weight:600">'+(s.consensus!=null?s.consensus+"F":"--")+'</td>'
      +'<td style="color:var(--green);font-weight:600">'+(s.implied!=null?s.implied+"F":"--")+'</td>'
      +'<td style="color:'+pc+'">'+paceStr+'</td>'
      +'<td style="color:var(--yellow)">'+(s.obs!=null?s.obs+"F":"--")+'</td></tr>';
  }).join("");
}

function loadHistory(){
  fetch("/api/history?station="+STATION).then(function(r){ return r.json(); }).then(function(history){
    var dates = Object.keys(history).sort().reverse();
    var thead = document.getElementById("hist-thead");
    var tbody = document.getElementById("hist-tbody");
    var countEl = document.getElementById("hist-count");
    if(!dates.length){
      tbody.innerHTML = '<tr><td colspan="2" style="color:var(--dim)">No history yet. Data accumulates after each day rolls over.</td></tr>';
      return;
    }
    var allModels = [];
    dates.forEach(function(d){ Object.keys(history[d].avg_pace).forEach(function(m){ if(allModels.indexOf(m)<0) allModels.push(m); }); });
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
</script>
</body>
</html>
"""

_started = False
_start_lock = threading.Lock()


def load_accuracy(station):
    data = load_json_file(f"{DATA_DIR}/accuracy_{station}.json", {})
    if data:
        get_state(station)["accuracy"] = data


def start_background():
    global _started
    with _start_lock:
        if not _started:
            _started = True
            load_active_stations()
            for station in STATION_POOL:
                load_accuracy(station)
            # Route through the same cross-process ownership check used by the
            # watchdog (see BUG P1 cross-process fix above) — only the worker
            # that wins the flock on _BGLOOP_LOCKFILE actually starts a
            # bgloop thread here. Every other worker still initializes its
            # local state (active stations, accuracy data) above, since that
            # in-memory state is per-process and each worker needs it to
            # serve /api/state etc. — it just won't independently fetch.
            _ensure_background_thread_running()
            print(f"start_background() complete for pid {os.getpid()}", flush=True)


with app.app_context():
    start_background()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
