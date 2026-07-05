import os, json, time, threading, random, math
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

# --- Rate limiting: max requests per second to wethr API ---
_api_lock = threading.Lock()
_last_request_time = 0
MIN_REQUEST_INTERVAL = 2.5  # seconds between API calls

# --- Manual refresh cooldown: stops external pings / rapid re-clicks from
# bypassing REFRESH_SEC and spawning unlimited fetch_all() runs ---
_manual_refresh_lock = threading.Lock()
_last_manual_refresh = {}
MANUAL_REFRESH_COOLDOWN_SEC = 120  # min seconds between manual refreshes, per station

# --- Hard daily API cap: resets at 19:30 UTC (= 3:30pm EDT / 2:30pm EST) ---
DAILY_REQUEST_CAP = 4500
_CAP_RESET_UTC_HOUR = 19
_CAP_RESET_UTC_MINUTE = 30
_counter_lock = threading.Lock()

class DailyCapExceeded(Exception):
    pass

def _get_period_key():
    """Returns string key for the current quota period.
    Resets at 19:30 UTC (3:30pm EDT in summer; shifts 1hr in winter — acceptable).
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
    """Raises DailyCapExceeded if at or over cap; otherwise increments and saves."""
    with _counter_lock:
        period = _get_period_key()
        data = _load_api_counter()
        count = data.get(period, 0)
        if count >= DAILY_REQUEST_CAP:
            raise DailyCapExceeded(f"Daily cap ({DAILY_REQUEST_CAP}) reached. Resets at 3:30pm EST.")
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

STATIONS = ["KOKC", "KPHL", "KDCA", "KBOS", "KDEN", "KHOU", "KLAS", "KMDW", "KMSP", "KSAT"]
STATION_NAMES = {
    "KOKC": "Oklahoma City Will Rogers World Airport",
    "KPHL": "Philadelphia International Airport",
    "KDCA": "Washington Reagan National Airport",
    "KBOS": "Boston Logan International Airport",
    "KDEN": "Denver International Airport",
    "KHOU": "Houston William P. Hobby Airport",
    "KLAS": "Las Vegas Harry Reid International Airport",
    "KMDW": "Chicago Midway International Airport",
    "KMSP": "Minneapolis-Saint Paul International Airport",
    "KSAT": "San Antonio International Airport",
}
STATION_TZ_OFFSET = {
    "KOKC": -6,
    "KPHL": -5,
    "KDCA": -5,
    "KBOS": -5,
    "KDEN": -7,
    "KHOU": -6,
    "KLAS": -8,
    "KMDW": -6,
    "KMSP": -6,
    "KSAT": -6,
}
# Only these stations are fetched automatically by the background loop.
# All 10 are valid and can be fetched on demand via the NOW button.
BACKGROUND_STATIONS = ["KOKC", "KPHL", "KDCA"]

# --- Nowcast / conditions constants ---
# Sky cover boost tiers (oktas -> F bonus): applied to solar-adjusted nowcast
_SKY_BOOST = [
    (0, 2,  3.0),   # CLR / FEW  (0-2 oktas): full solar boost
    (3, 4,  1.5),   # SCT        (3-4 oktas): partial boost
    (5, 6,  0.5),   # BKN        (5-6 oktas): small boost
    (7, 8,  0.0),   # OVC/OVX    (7-8 oktas): no boost
]
# Station coordinates for solar elevation calculation
STATION_LAT = {
    "KOKC": 35.3931,
    "KPHL": 39.8719,
    "KDCA": 38.8521,
    "KBOS": 42.3629,
    "KDEN": 39.8561,
    "KHOU": 29.6454,
    "KLAS": 36.0800,
    "KMDW": 41.7868,
    "KMSP": 44.8848,
    "KSAT": 29.5337,
}
STATION_LON = {
    "KOKC": -97.6007,
    "KPHL": -75.2408,
    "KDCA": -77.0377,
    "KBOS": -71.0052,
    "KDEN": -104.6737,
    "KHOU": -95.2789,
    "KLAS": -115.1523,
    "KMDW": -87.7524,
    "KMSP": -93.2218,
    "KSAT": -98.4700,
}
# Per-station wind profiles.
# suppress: directions that reduce solar heating boost (factor 0.0)
# enhance:  directions that increase solar heating (factor applied on top of sky boost)
# neutral directions (not listed) leave boost unchanged (factor 1.0)
# enhance_factor: multiplier applied to base boost when enhancing wind present
STATION_WIND_PROFILE = {
    "KOKC": {
        "suppress": ["N","NNE","NNW","NE","NW"],
        "enhance":  ["S","SSW","SW","WSW"],
        "enhance_factor": 1.3,
    },
    "KPHL": {
        "suppress": ["SE","SSE","ESE","NW","NNW"],  # sea breeze suppresses; post-frontal NW suppresses
        "enhance":  ["SW","WSW","W"],
        "enhance_factor": 1.2,
    },
    "KDCA": {
        "suppress": ["E","ESE","SE"],               # Potomac sea breeze suppresses
        "enhance":  ["SW","WSW","S"],               # SW flow enhances significantly
        "enhance_factor": 1.3,
    },
    "KBOS": {
        "suppress": ["N","NNE","NE","ENE","E","SE"], # sea breeze + northerly suppress
        "enhance":  ["SW","WSW","W","NW"],
        "enhance_factor": 1.2,
    },
    "KDEN": {
        "suppress": ["E","ENE","ESE","NE"],          # upslope suppresses (clouds/precip)
        "enhance":  ["W","WSW","WNW","SW"],          # chinook downslope enhances
        "enhance_factor": 1.5,
    },
    "KHOU": {
        "suppress": ["S","SSE","SE","SSW"],          # Gulf moisture suppresses max temp
        "enhance":  ["N","NNW","NW","W"],            # dry northerly post-frontal enhances
        "enhance_factor": 1.2,
    },
    "KLAS": {
        "suppress": ["N","NNE","NE","E","ENE"],      # northerly/monsoon moisture suppresses
        "enhance":  ["W","WSW","SW","S"],            # desert SW flow enhances
        "enhance_factor": 1.4,
    },
    "KMDW": {
        "suppress": ["N","NNE","NE","ENE","E"],      # Lake Michigan lake breeze suppresses
        "enhance":  ["SW","WSW","W","S"],
        "enhance_factor": 1.2,
    },
    "KMSP": {
        "suppress": ["N","NNE","NW","NNW"],          # post-frontal northerly suppresses sharply
        "enhance":  ["SW","WSW","S","SSW"],
        "enhance_factor": 1.2,
    },
    "KSAT": {
        "suppress": ["S","SSE","SE","SSW"],          # Gulf moisture suppresses
        "enhance":  ["N","NNW","NW","W"],            # dry northerly enhances
        "enhance_factor": 1.2,
    },
}

ALL_KNOWN_MODELS = [
    "ARPEGE","HRRR","UKMO","LAV-MOS","NAM","RAP","GEM-GDPS","NAM-MOS","NBM",
    "NAM4KM","GFS","ICON","GFS-MOS","NBS-MOS","ECMWF-HRES","GEFS","JMA","RDPS","SREF"
]
RUN_CYCLES = ["00Z","01Z","02Z","03Z","04Z","05Z","06Z","07Z","08Z","09Z","10Z","11Z",
              "12Z","13Z","14Z","15Z","16Z","17Z","18Z","19Z","20Z","21Z","22Z","23Z"]
REFRESH_SEC = 1800  # 30 min between auto-refresh cycles; use NOW button for on-demand updates

# Quiet hours: auto-fetch (background loop) is skipped during this window to conserve
# the shared wethr.net daily API cap. Manual refresh (NOW button, /api/refresh) is NOT
# affected and works 24/7. Window is checked against a single reference station's local
# clock (not per-station) so all auto-fetch pauses/resumes at the same wall-clock time.
QUIET_HOURS_START = 19   # 7pm local (24h)
QUIET_HOURS_END = 5      # 5am local (24h)
QUIET_HOURS_REF_STATION = "KOKC"

# Typical lag between solar noon and the actual daily high (climatological default;
# actual lag varies by station/season/air mass, but 3.5hr is a reasonable general value
# consistent with the ~4:30-4:40pm typical-high windows seen for solar noon ~1:10-1:15pm).
TYPICAL_PEAK_LAG_HOURS = 3.5

# Minimum spacing between obs samples used for rate calculations. Prevents a pair
# of back-to-back manual refreshes (seconds/minutes apart) from producing a wild
# divide-by-near-zero warming rate.
MIN_INTERVAL_HOURS = 0.15  # ~9 minutes

def in_quiet_hours():
    hour = station_local_now(QUIET_HOURS_REF_STATION).hour
    if QUIET_HOURS_START > QUIET_HOURS_END:
        return hour >= QUIET_HOURS_START or hour < QUIET_HOURS_END
    return QUIET_HOURS_START <= hour < QUIET_HOURS_END

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
        "metar": None,          # raw METAR dict from aviationweather.gov
        "solar_noon_obs": None, # obs temp recorded near solar noon (float or None)
        "solar_noon_dt": None,  # UTC datetime string when solar noon obs was recorded
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

def _throttle():
    """Enforce minimum interval between API calls (global, across all stations)."""
    global _last_request_time
    with _api_lock:
        now = time.monotonic()
        gap = now - _last_request_time
        if gap < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - gap)
        _last_request_time = time.monotonic()

def wethr_get(path, retries=3):
    """
    Rate-limited GET with exponential backoff retry on 429/5xx.
    Raises DailyCapExceeded immediately if the hard daily cap is reached.
    """
    _check_and_increment()  # raises DailyCapExceeded before any sleep/request
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
                print(f"[429] Rate limited on {path}. Waiting {wait:.1f}s (attempt {attempt+1}/{retries})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if attempt < retries - 1 and e.response is not None and e.response.status_code in (429, 500, 502, 503):
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
    for k in ["temperature_f","temperature_display","temperature","temp","value","high",
              "max_temp","max_temperature","temp_f","temp_max","forecast_high",
              "temperature_high","t","fahrenheit","f"]:
        v = x.get(k)
        if v is not None:
            try: return round(float(v), 1)
            except: pass
    # Last resort: find any numeric-looking value in the dict that's plausibly a temp
    for k, v in x.items():
        if k in ("valid_time","run_time","run","model","station","date","time","hour","id","type"):
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
    vt = str(x.get("valid_time",""))
    try: return datetime.strptime(vt[:16], "%Y-%m-%d %H:%M")
    except: return None

def station_local_now(station="KOKC"):
    offset = STATION_TZ_OFFSET.get(station, -6)
    return datetime.utcnow() + timedelta(hours=offset)

def station_day_bounds(station="KOKC", offset=0):
    tz_offset = STATION_TZ_OFFSET.get(station, -6)
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

def _safe_float(v):
    """Convert v to float, returning None for None/NaN/Inf (safe for JSON serialization)."""
    if v is None:
        return None
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None

def deg_to_cardinal(deg):
    """Convert wind degrees to 16-point cardinal string, e.g. 337 -> 'NNW'."""
    try:
        d = float(deg) % 360
    except (TypeError, ValueError):
        return None
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
            "S","SSW","SW","WSW","W","WNW","NW","NNW"]
    return dirs[int((d + 11.25) / 22.5) % 16]

def fetch_metar(station):
    """
    Fetch the latest METAR for station from aviationweather.gov.
    Uses plain requests.get — NOT wethr_get — so it never touches the daily cap.
    Returns a dict with keys: raw, temp_c, wind_dir, wind_speed_kt, sky_oktas, flight_category.
    Returns None on any failure.
    """
    try:
        url = f"https://aviationweather.gov/api/data/metar?ids={station}&format=json&taf=false"
        r = requests.get(url, timeout=8)
        r.raise_for_status()
        data = r.json()
        if not data or not isinstance(data, list):
            return None
        m = data[0]
        # Sky cover: take the highest BKN/OVC/OVX layer, else highest SCT/FEW
        sky_oktas = None
        sky_layers = m.get("clouds") or []
        cover_map = {"CLR": 0, "SKC": 0, "FEW": 1, "SCT": 3, "BKN": 5, "OVC": 7, "OVX": 8}
        max_cover = -1
        for layer in sky_layers:
            cov = layer.get("cover", "")
            val = cover_map.get(cov, -1)
            if val > max_cover:
                max_cover = val
        if max_cover >= 0:
            sky_oktas = max_cover
        wind_dir_deg = m.get("wdir")
        wind_card = deg_to_cardinal(wind_dir_deg)
        return {
            "raw": m.get("rawOb", ""),
            "temp_c": _safe_float(m.get("temp")),
            "wind_dir": wind_card,
            "wind_dir_deg": _safe_float(wind_dir_deg),
            "wind_speed_kt": _safe_float(m.get("wspd")),
            "sky_oktas": sky_oktas,
            "flight_category": m.get("fltcat", ""),
            "obs_time": m.get("obsTime", ""),
        }
    except Exception as e:
        return None

def solar_noon_utc(station, date_utc=None):
    """Compute solar noon in UTC using equation of time. Returns datetime (UTC)."""
    if date_utc is None:
        date_utc = datetime.utcnow().date()
    lon = STATION_LON.get(station, -90.0)
    doy = date_utc.timetuple().tm_yday
    B = math.radians((360 / 365) * (doy - 81))
    eot = 9.87 * math.sin(2 * B) - 7.53 * math.cos(B) - 1.5 * math.sin(B)
    lon_correction_min = lon * 4
    solar_noon_utc_min = 720 - lon_correction_min - eot
    h = int(solar_noon_utc_min // 60) % 24
    mins = int(solar_noon_utc_min % 60)
    return datetime(date_utc.year, date_utc.month, date_utc.day, h, mins, 0)

def solar_elevation_deg(station, dt_utc):
    """Compute solar elevation angle in degrees for station at given UTC datetime."""
    lat = math.radians(STATION_LAT.get(station, 35.0))
    lon = STATION_LON.get(station, -90.0)
    doy = dt_utc.timetuple().tm_yday
    # Solar declination
    decl = math.radians(23.45 * math.sin(math.radians((360 / 365) * (doy - 81))))
    # Hour angle: degrees from solar noon
    B = math.radians((360 / 365) * (doy - 81))
    eot = 9.87 * math.sin(2 * B) - 7.53 * math.cos(B) - 1.5 * math.sin(B)  # minutes
    solar_time_min = (dt_utc.hour * 60 + dt_utc.minute) + lon * 4 + eot
    hour_angle = math.radians((solar_time_min / 4) - 180)
    # Elevation
    sin_elev = (math.sin(lat) * math.sin(decl) +
                math.cos(lat) * math.cos(decl) * math.cos(hour_angle))
    return math.degrees(math.asin(max(-1.0, min(1.0, sin_elev))))

def compute_nowcast(station, st):
    """
    Compute a solar-adjusted nowcast high from the current obs temp.
    - Sky cover boost from METAR oktas
    - Time scaling based on true solar elevation angle (peaks at solar noon)
    - Per-station wind profile: suppress/enhance boost based on wind direction
    - Wind speed continuous suppression: full boost calm, zero boost >= 25kt
    Returns a dict or None on failure.
    """
    try:
        obs = st.get("obs") or {}
        obs_temp = _safe_float(obs.get("temperature_display"))
        if obs_temp is None:
            return None
        metar = st.get("metar") or {}
        sky_oktas = metar.get("sky_oktas")
        wind_dir = metar.get("wind_dir")
        wind_kt = _safe_float(metar.get("wind_speed_kt")) or 0.0

        now_utc = datetime.utcnow()
        noon_utc = solar_noon_utc(station, now_utc.date())
        hours_to_noon = (noon_utc - now_utc).total_seconds() / 3600.0

        # Solar noon is deterministic and known in advance for each station.
        # Once it has passed, the heating phase is over — no projection is made.
        # The observed max stands on its own from this point forward.
        if hours_to_noon < 0:
            return None

        # Solar elevation at current time and at noon — use ratio as time factor
        elev_now = solar_elevation_deg(station, now_utc)
        elev_noon = solar_elevation_deg(station, noon_utc)
        if elev_noon > 0 and elev_now > 0:
            time_factor = min(1.0, elev_now / elev_noon)
        else:
            time_factor = max(0.0, min(1.0, hours_to_noon / 4.0))

        # Sky cover base boost
        boost = 1.0  # unknown sky: modest default
        if sky_oktas is not None:
            for lo, hi, b in _SKY_BOOST:
                if lo <= sky_oktas <= hi:
                    boost = b
                    break

        # Per-station wind direction factor
        wind_effect = "neutral"
        profile = STATION_WIND_PROFILE.get(station, {})
        if wind_dir:
            if wind_dir in profile.get("suppress", []):
                boost = 0.0
                wind_effect = "suppress"
            elif wind_dir in profile.get("enhance", []):
                boost = boost * profile.get("enhance_factor", 1.2)
                wind_effect = "enhance"

        # Wind speed continuous suppression (independent of direction effect)
        # Full boost calm -> zero boost at 25kt+
        if wind_effect != "suppress":
            speed_factor = max(0.0, 1.0 - (wind_kt / 25.0))
            boost = boost * speed_factor

        scaled_boost = round(boost * time_factor, 1)
        nowcast = round(obs_temp + scaled_boost, 1)
        return {
            "nowcast": nowcast,
            "obs_temp": obs_temp,
            "solar_noon_utc": noon_utc.strftime("%H:%MZ"),
            "solar_elevation": round(elev_now, 1),
            "solar_elevation_noon": round(elev_noon, 1),
            "hours_to_noon": round(hours_to_noon, 2),
            "time_factor": round(time_factor, 2),
            "sky_boost": scaled_boost,
            "wind_effect": wind_effect,
            "wind_dir": wind_dir,
            "wind_kt": wind_kt,
            "sky_oktas": sky_oktas,
        }
    except Exception:
        return None

STALE_DUP_MINUTES = 15  # if two readings show the identical temp within this window,
                         # treat them as the same underlying station ob, not new data

# Sanity cap on how much warming any "we're not sure yet, bridge forward" fallback
# is allowed to add. A single noisy short interval (e.g. 12 min) can produce an
# implausible instantaneous rate; without this cap, bridging that rate forward
# even 1.5hr can blow past every model's forecast.
MAX_BRIDGE_ADDED_F = 6.0

def get_today_obs_samples(station="KOKC"):
    """
    Returns today's [(local_datetime, obs_temp), ...] built from pacing snapshot
    history (populated every fetch_all cycle, auto or manual). Sorted ascending.

    Dedupes two ways:
      1. By the station's obs_time field, when present and actually changing.
      2. By value: if the temp is identical to the last kept sample and the two
         are within STALE_DUP_MINUTES of each other, skip it. This is the primary
         real-world guard — ASOS/METAR obs often refresh slower than our poll
         cadence, and some upstream APIs stamp observation_time with the API
         response time rather than the true station reading time, which makes
         guard #1 alone unreliable. Without this, repeated identical readings
         look like "warming rate = 0" and falsely trigger "already peaked."
    """
    now = station_local_now(station)
    date_str = now.strftime("%Y-%m-%d")
    disk = load_json_file(f"{DATA_DIR}/pacing_{station}.json", {})
    day = disk.get(date_str, [])
    samples = []
    last_obs_time = None
    last_kept_dt = None
    last_kept_temp = None
    for e in day:
        t = e.get("time")
        obs = e.get("obs")
        obs_time = e.get("obs_time")
        if t is None or obs is None:
            continue
        try:
            hh, mm = t.split(":")
            dt = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
            obs_f = float(obs)
        except Exception:
            continue

        if obs_time is not None and obs_time == last_obs_time:
            continue  # confirmed same underlying station reading

        if (last_kept_temp is not None and obs_f == last_kept_temp
                and last_kept_dt is not None
                and abs((dt - last_kept_dt).total_seconds()) / 60.0 < STALE_DUP_MINUTES):
            continue  # identical temp within a short window — source likely hasn't refreshed

        samples.append((dt, obs_f))
        last_kept_dt = dt
        last_kept_temp = obs_f
        if obs_time is not None:
            last_obs_time = obs_time
    samples.sort(key=lambda x: x[0])
    return samples

def compute_today_high_projection(station="KOKC"):
    """
    Projects today's high from the OBSERVED warming-rate trend, not a fixed solar
    curve. Builds hourly warming-rate intervals between consecutive obs samples
    (skipping intervals shorter than MIN_INTERVAL_HOURS to avoid noise from
    back-to-back manual refreshes), then:
      1. If the most recent rate is >= the prior rate, we're still in the
         still-accelerating morning phase — use a conservative capped bridge
         instead of extrapolating an undefined decay curve.
      2. If the rate is already <= 0, warming has stopped/reversed — today's
         obs stands as the high.
      3. Otherwise, fit an exponential decay. With >=3 clean positive-rate
         intervals, fit via least-squares regression across the last 5 (more
         stable — one noisy sample can't flip the whole projection). With
         exactly 2 intervals, fall back to the direct two-point decay constant.
      Integrates the decaying rate forward to the typical high time (solar
      noon + TYPICAL_PEAK_LAG_HOURS).
    Returns a dict, or {"error": ...} if there isn't enough data yet.
    """
    samples = get_today_obs_samples(station)
    if len(samples) < 3:
        return {"error": "insufficient_data", "samples": len(samples), "need": 3}

    intervals = []  # (midpoint_time, rate_F_per_hr, dt_hr)
    for i in range(1, len(samples)):
        t_a, T_a = samples[i - 1]
        t_b, T_b = samples[i]
        dt = (t_b - t_a).total_seconds() / 3600.0
        if dt < MIN_INTERVAL_HOURS:
            continue
        rate = (T_b - T_a) / dt
        mid = t_a + (t_b - t_a) / 2
        intervals.append((mid, rate, dt))

    if len(intervals) < 2:
        return {"error": "insufficient_spaced_data", "samples": len(samples)}

    t_last, T_last = samples[-1]
    now_local = station_local_now(station)
    noon_utc = solar_noon_utc(station, now_local.date())
    offset = STATION_TZ_OFFSET.get(station, -6)
    noon_local = noon_utc + timedelta(hours=offset)
    peak_time_local = noon_local + timedelta(hours=TYPICAL_PEAK_LAG_HOURS)
    remaining_hours = (peak_time_local - t_last).total_seconds() / 3600.0

    samples_used = [{"time": t.strftime("%H:%M"), "temp": v} for t, v in samples[-4:]]

    if remaining_hours <= 0:
        return {
            "projected_high": T_last, "added_warming": 0.0, "method": "past_peak_window",
            "obs_now": T_last, "remaining_hours": round(remaining_hours, 2),
            "peak_time_local": peak_time_local.strftime("%I:%M%p").lstrip("0"),
            "samples_used": samples_used,
        }

    rate_latest = intervals[-1][1]
    rate_prev = intervals[-2][1]

    if rate_latest <= 0:
        if rate_prev <= 0:
            method = "already_peaked"
            added = 0.0
            projected_high = T_last
        else:
            # A single flat/negative interval right after a positive one is
            # more likely short-term noise (cloud, obs blip) than a confirmed
            # peak — don't fully commit to "already peaked" off one reading.
            # Bridge forward cautiously using half the prior confirmed rate.
            method = "plateau_unconfirmed"
            bridge_hours = min(remaining_hours, 1.5)
            added = min(0.5 * rate_prev * bridge_hours, MAX_BRIDGE_ADDED_F)
            projected_high = round(T_last + added, 1)
    elif rate_prev <= 0 or rate_latest >= rate_prev:
        method = "linear_bridge_capped"
        bridge_hours = min(remaining_hours, 1.5)
        added = min(rate_latest * bridge_hours, MAX_BRIDGE_ADDED_F)
        projected_high = round(T_last + added, 1)
    else:
        # Only fit the decay using the monotonically non-increasing tail of
        # intervals (walking back from the latest). This is essential: the
        # rate often accelerates through the morning before it starts
        # decaying, and blending that acceleration phase into the regression
        # would understate how sharply the rate just turned over.
        tail = [intervals[-1]]
        for iv in reversed(intervals[:-1]):
            if iv[1] >= tail[0][1]:
                tail.insert(0, iv)
            else:
                break
        recent = [iv for iv in tail if iv[1] > 0]

        if len(recent) >= 3:
            method = "decay_integration_regression"
            t0 = recent[0][0]
            xs = [(m - t0).total_seconds() / 3600.0 for m, r, dt in recent]
            ys = [math.log(r) for m, r, dt in recent]
            n = len(xs)
            mean_x = sum(xs) / n
            mean_y = sum(ys) / n
            num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
            den = sum((x - mean_x) ** 2 for x in xs)
            slope = (num / den) if den > 0 else -0.01
            k = max(-slope, 0.01)
            intercept = mean_y - slope * mean_x
            x_last = (t_last - t0).total_seconds() / 3600.0
            rate_now = math.exp(intercept + slope * x_last)
        else:
            method = "decay_integration_2pt"
            k = math.log(rate_prev / rate_latest) / intervals[-1][2]
            k = max(k, 0.01)
            rate_now = rate_latest
        added = (rate_now / k) * (1 - math.exp(-k * remaining_hours))
        projected_high = round(T_last + added, 1)

    return {
        "projected_high": projected_high,
        "added_warming": round(added, 1),
        "method": method,
        "obs_now": T_last,
        "rate_latest": round(rate_latest, 2),
        "rate_prev": round(rate_prev, 2),
        "remaining_hours": round(remaining_hours, 2),
        "peak_time_local": peak_time_local.strftime("%I:%M%p").lstrip("0"),
        "samples_used": samples_used,
    }

def fetch_all(station="KOKC"):
    st = get_state(station)
    if not API_KEY:
        add_log("No API key set", "err", station)
        return
    # Check cap before doing anything
    try:
        _check_and_increment.__doc__  # just a harmless reference; real check is in wethr_get
        counter_data = _load_api_counter()
        period = _get_period_key()
        current_count = counter_data.get(period, 0)
        if current_count >= DAILY_REQUEST_CAP:
            add_log(f"Daily API cap reached ({current_count}/{DAILY_REQUEST_CAP}) — skipping fetch. Resets 3:30pm EST.", "warn", station)
            return
    except Exception:
        pass
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
    except DailyCapExceeded as e:
        add_log(f"Daily cap reached — stopping fetch. Resets 3:30pm EST.", "warn", station)
        return
    except Exception as e:
        errors.append(f"Obs: {e}")
        add_log(f"Obs error: {e}", "err", station)

    # Solar noon obs recording (read-only, no API call, cannot crash fetch_all)
    try:
        noon_utc = solar_noon_utc(station)
        now_utc = datetime.utcnow()
        diff_min = abs((now_utc - noon_utc).total_seconds()) / 60.0
        if diff_min <= 15 and st.get("obs"):
            solar_temp = _safe_float((st["obs"] or {}).get("temperature_display"))
            if solar_temp is not None:
                st["solar_noon_obs"] = solar_temp
                st["solar_noon_dt"] = now_utc.strftime("%Y-%m-%dT%H:%MZ")
                add_log(f"Solar noon obs recorded: {solar_temp}F at {st['solar_noon_dt']}", "info", station)
    except Exception as e:
        add_log(f"Solar noon obs error (non-fatal): {e}", "warn", station)

    # Wethr high
    try:
        wh = wethr_get(f"observations.php?station_code={station}&mode=wethr_high&logic=nws")
        st["wethr_high"] = wh
        add_log(f"Wethr High: {wh.get('wethr_high')}F", "ok", station)
    except DailyCapExceeded:
        add_log(f"Daily cap reached — stopping fetch. Resets 3:30pm EST.", "warn", station)
        return
    except Exception as e:
        errors.append(f"WethrHigh: {e}")
        add_log(f"Wethr High error: {e}", "err", station)

    fetch_targets = active_models(station)
    if not fetch_targets:
        add_log("No accuracy data yet — using all known models", "warn", station)
        fetch_targets = ALL_KNOWN_MODELS

    utc_now = datetime.utcnow()
    tz_offset = STATION_TZ_OFFSET.get(station, -6)

    # Sequential model fetches with throttling (handled inside wethr_get)
    for model in fetch_targets:
        try:
            data = wethr_get(f"forecasts.php?location_name={station}&model={requests.utils.quote(model)}&run=latest")
            temps = data if isinstance(data, list) else data.get("forecasts", [])
            meta = {} if isinstance(data, list) else data
            if temps:
                # Log the raw keys of the first entry so we can see the API shape
                if temps:
                    sample = temps[0]
                    add_log(f"{model} sample keys: {list(sample.keys())} | vals: {dict(list(sample.items())[:6])}", "info", station)
                todays = today_entries(temps, station)
                if not todays:
                    add_log(f"{model}: no entries for today", "warn", station)
                    continue
                min_entries = 12 if model == "HRRR" else 4
                if len(todays) < min_entries:
                    add_log(f"{model}: only {len(todays)} entries for today — run not fully ingested yet, keeping previous", "warn", station)
                    continue
                max_entry = max(todays, key=lambda x: get_temp(x) or 0)
                raw_temp = get_temp(max_entry)
                closest = min(todays, key=lambda x: abs((parse_vt(x) - utc_now).total_seconds()) if parse_vt(x) else 99999)
                current_temp = get_temp(closest)
                run_raw = meta.get("run_time") or max_entry.get("run_time") or max_entry.get("run") or ""
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

                st["forecasts"][model] = {
                    "high": raw_temp,
                    "current_fcst": current_temp,
                    "run": run_fmt,
                    "tmr_high": tmr_temp,
                    "tmr_low": tmr_low,
                    "tmr_low_time": tmr_low_time,
                    # Conditions fields extracted from closest forecast entry
                    "conditions": _safe_float(closest.get("weather_code") or closest.get("condition_code")),
                    "cloud_cover": _safe_float(closest.get("cloud_cover") or closest.get("total_cloud_cover")),
                }
                if raw_temp is None:
                    add_log(f"{model}: WARNING raw_temp=None — check sample keys above. entry keys={list(max_entry.keys())}", "warn", station)
                else:
                    add_log(f"{model}: high={raw_temp} now={current_temp} run={run_fmt} ({len(todays)} entries)", "ok", station)
        except DailyCapExceeded:
            add_log(f"Daily cap reached mid-fetch — stopping. Resets 3:30pm EST.", "warn", station)
            break
        except Exception as e:
            errors.append(f"{model}: {e}")
            add_log(f"{model} error: {str(e)}", "warn", station)
            print(f"FULL ERROR for {model}: {e}", flush=True)

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
        now_local = station_local_now(station)
        if now_local.minute < 10 or (now_local.minute >= 30 and now_local.minute < 40):
            save_consensus_snapshot(station)
    except Exception as e:
        add_log(f"Consensus snapshot error: {e}", "warn", station)

    # METAR fetch — uses aviationweather.gov (free), NOT wethr_get, cannot touch daily cap
    try:
        metar = fetch_metar(station)
        if metar:
            st["metar"] = metar
            add_log(f"METAR: sky={metar.get('sky_oktas')} oktas wind={metar.get('wind_dir')} cat={metar.get('flight_category')}", "info", station)
        else:
            add_log("METAR: no data returned (non-fatal)", "warn", station)
    except Exception as e:
        add_log(f"METAR fetch error (non-fatal): {e}", "warn", station)


_memory_snapshots = {}

def save_pacing_snapshot(rows, station="KOKC"):
    st = get_state(station)
    now = station_local_now(station)
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M")

    entry = {"time": time_str}
    obs_temp_snap = _safe_float((st.get("obs") or {}).get("temperature_display"))
    if obs_temp_snap is not None:
        entry["obs"] = obs_temp_snap
        entry["obs_time"] = (st.get("obs") or {}).get("observation_time")
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
            avg[m] = round(sum(vals)/len(vals), 2)
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

    add_log(f"Snapshot: {len([r for r in rows if r.get('pace') is not None])} models | avg pace sample: {list(avg.items())[:3]}", "info", station)

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
            daily_avg[m] = round(sum(vals)/len(vals), 2)
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
            pace = round(float(obs_temp) - float(current_fcst), 2) if obs_temp and current_fcst else None
        except:
            pace = None
        rows.append({"model": model, "pace": pace})
    return rows

def scheduled_fetch():
    """
    Auto-fetch background stations only. All 10 stations are valid and fetchable
    on demand via the NOW button; only BACKGROUND_STATIONS run automatically.
    Skipped entirely during quiet hours (see QUIET_HOURS_START/END) to save API quota;
    the NOW button still works normally during quiet hours.
    """
    if in_quiet_hours():
        add_log(
            f"Quiet hours ({QUIET_HOURS_START}:00-{QUIET_HOURS_END}:00 {QUIET_HOURS_REF_STATION} local) — skipping auto-fetch",
            "info", QUIET_HOURS_REF_STATION
        )
        return
    for i, station in enumerate(BACKGROUND_STATIONS):
        if i > 0:
            gap = 10 + random.uniform(2, 5)
            add_log(f"Waiting {gap:.0f}s before fetching next station", "info", BACKGROUND_STATIONS[i-1])
            time.sleep(gap)
        try:
            fetch_all(station)
        except Exception as e:
            add_log(f"scheduled_fetch error: {e}", "err", station)

def weighted_consensus(items, decimals=1):
    """
    Weighted average of model values using inverse-variance-style weighting:
        weight = 1 / (MAE * RMSE)
    instead of plain 1/MAE. This penalizes models that are both biased AND
    volatile much harder than MAE alone (e.g. a model with a great MAE but
    a wide RMSE stops dominating consensus).

    Also applies a median/MAD outlier gate: any model whose value sits more
    than 3x the group's MAD away from the median is dropped from the
    average entirely (still shown in the UI table, just excluded from the
    blend). MAD is floored at 1.0F so a tight, well-agreeing pack doesn't
    trigger false exclusions.

    items: list of {"value": float, "mae": float, "rmse": float|None}
    Returns rounded float or None if nothing valid.
    """
    valid = []
    for it in items:
        try:
            v = float(it.get("value"))
            mae = float(it.get("mae") or 0)
            if mae <= 0:
                continue
        except (TypeError, ValueError):
            continue
        try:
            rmse = float(it.get("rmse") or 0)
            if rmse <= 0:
                rmse = mae  # fall back to MAE-only weighting if RMSE missing
        except (TypeError, ValueError):
            rmse = mae
        valid.append({"value": v, "mae": mae, "rmse": rmse})

    if not valid:
        return None

    n = len(valid)
    vals = sorted(x["value"] for x in valid)
    median = vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2
    devs = sorted(abs(x["value"] - median) for x in valid)
    mad = devs[n // 2] if n % 2 else (devs[n // 2 - 1] + devs[n // 2]) / 2
    mad = max(mad, 1.0)

    filtered = [x for x in valid if abs(x["value"] - median) <= 3 * mad] if n >= 4 else valid
    if not filtered:
        filtered = valid  # never fall through to nothing

    w_sum, w_total = 0.0, 0.0
    for x in filtered:
        w = 1 / (x["mae"] * x["rmse"])
        w_sum += x["value"] * w
        w_total += w

    return round(w_sum / w_total, decimals) if w_total > 0 else None

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
    cons_items = []
    pace_items = []
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
        mae_val = run_data.get("mae") if run_data else None
        if not mae_val:
            mae_val = a.get("mae")
        rmse_val = run_data.get("rmse") if run_data else None
        if not rmse_val:
            rmse_val = a.get("rmse")
        try:
            adj = round(float(raw) + float(corr), 1) if raw is not None and corr not in (None, "") else None
            if adj is not None:
                cons_items.append({"value": adj, "mae": mae_val, "rmse": rmse_val})
        except: pass
        try:
            current_fcst = fcst.get("current_fcst")
            pace = round(float(obs_temp) - float(current_fcst), 2) if obs_temp and current_fcst else None
            if pace is not None:
                pace_items.append({"value": pace, "mae": mae_val, "rmse": rmse_val})
        except: pass
    consensus = weighted_consensus(cons_items)
    cons_pace = weighted_consensus(pace_items, decimals=2)
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
            for k in keys[:-90]: del disk[k]
        save_json_file(path, disk)
    except Exception as e:
        add_log(f"Consensus snapshot error: {e}", "warn", station)

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
            for station in STATIONS:
                now = station_local_now(station)
                if now.hour == 1:
                    rollup_daily_history(station)
        except Exception as e:
            print(f"Rollup error: {e}")
        time.sleep(REFRESH_SEC)

def _get_prev_days(n, station="KOKC"):
    history = load_json_file(f"{DATA_DIR}/history_{station}.json", {})
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
        current_run = fcst.get("run","")

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

        try: adj = round(float(raw) + float(corr), 1) if raw is not None and corr not in (None,"") else None
        except: adj = None
        obs_temp = (st["obs"] or {}).get("temperature_display")
        current_fcst = fcst.get("current_fcst")
        try: pace = round(float(obs_temp) - float(current_fcst), 1) if obs_temp and current_fcst else None
        except: pace = None
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
            "corr_source": corr_source,   # "run", "default", or "overall"
            "adj_high": adj, "pace": pace,
            "tmr_high": tmr_raw, "tmr_adj": tmr_adj,
            "tmr_low": tmr_low, "tmr_low_adj": tmr_low_adj, "tmr_low_time": tmr_low_time,
            "mae": display_mae, "rmse": a.get("rmse"),
            "runs": a.get("runs", {}),
            "conditions": _safe_float(fcst.get("conditions")),
            "cloud_cover": _safe_float(fcst.get("cloud_cover")),
        })

    cons_items = []
    for r in rows:
        try:
            mae = float(r["mae"]); adj = r["adj_high"] if r["adj_high"] is not None else r["raw_high"]
            if adj is not None:
                cons_items.append({"value": float(adj), "mae": mae, "rmse": r.get("rmse")})
        except: pass
    consensus = weighted_consensus(cons_items)

    pace_items = []
    for r in rows:
        try:
            mae = float(r["mae"]); pace = r["pace"]
            if pace is not None:
                pace_items.append({"value": float(pace), "mae": mae, "rmse": r.get("rmse")})
        except: pass
    consensus_pace = weighted_consensus(pace_items, decimals=2)

    tmr_items = []
    for r in rows:
        try:
            mae = float(r["mae"]); tadj = r["tmr_adj"] if r["tmr_adj"] is not None else r["tmr_high"]
            if tadj is not None:
                tmr_items.append({"value": float(tadj), "mae": mae, "rmse": r.get("rmse")})
        except: pass
    tmr_consensus = weighted_consensus(tmr_items)

    # Conditions consensus + nowcast — isolated try/except; failure returns safe defaults
    conditions_data = {}
    nowcast_data = None
    try:
        cloud_vals = [r["cloud_cover"] for r in rows if r.get("cloud_cover") is not None]
        conditions_data = {
            "metar": st.get("metar"),
            "solar_noon_obs": _safe_float(st.get("solar_noon_obs")),
            "solar_noon_dt": st.get("solar_noon_dt"),
            "model_cloud_cover_avg": round(sum(cloud_vals) / len(cloud_vals), 1) if cloud_vals else None,
        }
        nowcast_data = compute_nowcast(station, st)
    except Exception:
        conditions_data = {}
        nowcast_data = None

    try:
        today_high_projection = compute_today_high_projection(station)
    except Exception as e:
        today_high_projection = {"error": str(e)}

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
        "conditions": conditions_data,
        "nowcast": nowcast_data,
        "today_high_projection": today_high_projection,
        "solar_noon_obs": _safe_float(st.get("solar_noon_obs")),
    })

@app.route("/api/history")
def api_history():
    station = request.args.get("station", "KOKC").upper()
    if station not in STATIONS:
        station = "KOKC"
    history = load_json_file(f"{DATA_DIR}/history_{station}.json", {})
    return jsonify(history)

@app.route("/api/accuracy", methods=["POST"])
def save_accuracy():
    station = request.args.get("station", "KOKC").upper()
    if station not in STATIONS:
        station = "KOKC"
    get_state(station)["accuracy"] = request.json or {}
    add_log("Accuracy data updated", "ok", station)
    save_json_file(f"{DATA_DIR}/accuracy_{station}.json", request.json or {})
    return jsonify({"ok": True})

@app.route("/api/consensus_snapshots")
def api_consensus_snapshots():
    station = request.args.get("station", "KOKC").upper()
    if station not in STATIONS: station = "KOKC"
    st = get_state(station)
    disk = load_json_file(f"{DATA_DIR}/consensus_{station}.json", {})
    return jsonify({
        "today": st.get("consensus_snapshots", []),
        "history": disk,
        "station": station,
    })
@app.route("/api/quota")
def api_quota():
    period = _get_period_key()
    data = _load_api_counter()
    count = data.get(period, 0)
    return jsonify({
        "period": period,
        "count": count,
        "cap": DAILY_REQUEST_CAP,
        "remaining": max(0, DAILY_REQUEST_CAP - count),
        "paused": count >= DAILY_REQUEST_CAP,
        "resets": "3:30pm EST daily",
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
    """Fetch one model raw and return the unprocessed API response for inspection."""
    station = request.args.get("station", "KOKC").upper()
    model = request.args.get("model", "HRRR")
    if station not in STATIONS:
        station = "KOKC"
    if not API_KEY:
        return jsonify({"error": "No API key set"})
    results = {}
    # Try every plausible parameter name combination
    attempts = [
        f"forecasts.php?station_code={station}&model={requests.utils.quote(model)}&run=latest",
        f"forecasts.php?location_code={station}&model={requests.utils.quote(model)}&run=latest",
        f"forecasts.php?station={station}&model={requests.utils.quote(model)}&run=latest",
        f"forecasts.php?station_code={station}&model={requests.utils.quote(model)}",
        f"forecasts.php?station_code={station}&model={requests.utils.quote(model)}&run=0",
    ]
    for url in attempts:
        try:
            data = wethr_get(url)
            temps = data if isinstance(data, list) else data.get("forecasts", [])
            sample = temps[:2] if temps else []
            results[url] = {
                "status": "OK",
                "response_type": type(data).__name__,
                "top_level_keys": list(data.keys()) if isinstance(data, dict) else "list",
                "total_entries": len(temps),
                "sample_entries": sample,
            }
            break  # stop at first success
        except Exception as e:
            results[url] = {"status": "ERROR", "error": str(e)}
    return jsonify(results)


@app.route("/api/refresh", methods=["POST"])
def manual_refresh():
    station = request.args.get("station", "KOKC").upper()
    if station not in STATIONS:
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
    return jsonify({"ok": True})

@app.before_request
def watchdog():
    for t in threading.enumerate():
        if t.name == "bgloop":
            return
    print("WATCHDOG: restarting background thread", flush=True)
    t = threading.Thread(target=background_loop, daemon=True, name="bgloop")
    t.start()

@app.route("/")
def index():
    return render_template_string(HTML)

HTML = r"""<!DOCTYPE html>
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
.stn-btn{border-radius:4px;padding:5px 12px;font-size:11px;cursor:pointer;font-family:inherit;letter-spacing:1px;transition:all .15s}
.stn-btn.active{background:#1e40af;border:1px solid #3b82f6;color:#93c5fd}
.stn-btn.inactive{background:none;border:1px solid #334155;color:#64748b}
/* Default run column highlight */
.default-col{background:#fb923c0d !important}
th.default-col{color:var(--orange) !important}
</style>
</head>
<body>
<header>
  <div>
    <h1 id="page-title">KOKC &middot; Model Tracker</h1>
    <div class="sub" id="page-sub">Oklahoma City Will Rogers World Airport</div>
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
    <div class="stat-pill" id="h-nowcast-pill" style="display:none">
      <div class="lbl">Nowcast High</div>
      <div class="val" id="h-nowcast" style="color:var(--orange)">--</div>
      <div class="sub2" id="h-nowcast-sub">solar adj</div>
    </div>
    <div class="sp"></div>
    <div style="display:flex;gap:6px;align-items:center" id="station-btns"></div>
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
    <div class="sc" id="s-nowcast-sc" style="display:none"><div class="lbl">Nowcast High</div><div class="v" id="s-nowcast" style="color:var(--orange)">--</div><div class="s" id="s-nowcast-sub">solar adj</div></div>
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

  <div class="card" id="conditions-card" style="display:none">
    <div class="ctitle">Conditions &amp; Nowcast</div>
    <div class="srow" style="margin-bottom:8px">
      <div class="sc"><div class="lbl">Flight Cat</div><div class="v" id="cond-fltcat" style="font-size:16px">--</div><div class="s">METAR</div></div>
      <div class="sc"><div class="lbl">Sky Cover</div><div class="v" id="cond-sky" style="font-size:16px">--</div><div class="s">oktas (METAR)</div></div>
      <div class="sc"><div class="lbl">Wind</div><div class="v" id="cond-wind" style="font-size:16px">--</div><div class="s" id="cond-wind-kt">-- kt</div></div>
      <div class="sc"><div class="lbl">Solar Noon Obs</div><div class="v" id="cond-noon-obs" style="color:var(--yellow);font-size:16px">--</div><div class="s" id="cond-noon-dt">--</div></div>
    </div>
    <div style="font-size:10px;color:var(--dimmer);margin-top:4px" id="cond-metar-raw"></div>
  </div>

  <div class="card" id="thp-card" style="display:none">
    <div class="ctitle">Today's High Projection <span style="color:var(--dimmer);font-weight:400">(rate-decay, obs-based)</span></div>
    <div class="srow" style="margin-bottom:4px">
      <div class="sc"><div class="lbl">Projected High</div><div class="v" id="thp-value" style="color:var(--orange);font-size:20px">--</div><div class="s" id="thp-method">--</div></div>
      <div class="sc"><div class="lbl">Latest Rate</div><div class="v" id="thp-rate-latest" style="font-size:16px">--</div><div class="s">F/hr</div></div>
      <div class="sc"><div class="lbl">Prior Rate</div><div class="v" id="thp-rate-prev" style="font-size:16px">--</div><div class="s">F/hr</div></div>
      <div class="sc"><div class="lbl">Typical Peak</div><div class="v" id="thp-peak-time" style="font-size:16px">--</div><div class="s" id="thp-remaining">--</div></div>
    </div>
    <div style="font-size:10px;color:var(--dimmer);margin-top:4px" id="thp-samples"></div>
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

  <!-- DEFAULT FALLBACK ENTRY -->
  <div class="card" style="border-color:#3a2a0a">
    <div class="ctitle" style="color:var(--orange)">&#9888; Default / Fallback Run Values</div>
    <p style="color:var(--dim);font-size:12px;line-height:1.7;margin-bottom:12px">
      Set a fallback MAE &amp; Correction per model. These apply automatically whenever a model's active run
      has <em>no</em> run-specific entry &mdash; keeping it out of consensus rather than polluting it with uncalibrated data.
      <br><span style="color:var(--orange)">D</span> badge in the dashboard Correction column indicates the default is active.
    </p>
    <div style="margin-bottom:14px;padding:10px;background:#1a1a2e;border:1px solid #334155;border-radius:6px">
      <div style="font-size:10px;color:var(--orange);letter-spacing:1px;margin-bottom:6px">&#9657; PASTE FROM WETHR.NET</div>
      <div style="font-size:11px;color:var(--dim);margin-bottom:8px">Paste the accuracy table from wethr.net — all models auto-filled in one shot.</div>
      <textarea id="paste-defaults-input" rows="6" style="width:100%;background:#0f0f1a;border:1px solid #334155;color:var(--text);border-radius:4px;padding:8px;font-size:11px;font-family:monospace;box-sizing:border-box;resize:vertical" placeholder="MODEL&#9;MAE&#9;CORRECTION&#9;RMSE&#9;DAYS&#10;NBM&#9;0.7°&#9;-0.5°F&#9;1.1°&#9;6&#10;HRRR&#9;1.1°&#9;+0.1°F&#9;1.5°&#9;6&#10;..."></textarea>
      <div style="display:flex;gap:8px;margin-top:8px;align-items:center">
        <button class="btn btn-blue" onclick="parseAndFillDefaults()">Fill From Paste</button>
        <button class="btn btn-blue" onclick="fillDefaultsFromLoaded()" style="margin-left:6px">Fill From Loaded Accuracy</button>
        <span id="paste-status" style="font-size:10px;color:var(--dim)"></span>
      </div>
    </div>
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
var STATION_LIST = ["KOKC","KPHL","KDCA","KBOS","KDEN","KHOU","KLAS","KMDW","KMSP","KSAT"];
var STATION_NAMES = {
  "KOKC": "Oklahoma City Will Rogers World Airport",
  "KPHL": "Philadelphia International Airport",
  "KDCA": "Washington Reagan National Airport",
  "KBOS": "Boston Logan International Airport",
  "KDEN": "Denver International Airport",
  "KHOU": "Houston William P. Hobby Airport",
  "KLAS": "Las Vegas Harry Reid International Airport",
  "KMDW": "Chicago Midway International Airport",
  "KMSP": "Minneapolis-Saint Paul International Airport",
  "KSAT": "San Antonio International Airport"
};
var STATION = localStorage.getItem("active_station") || "KOKC";
if(STATION_LIST.indexOf(STATION) === -1) STATION = "KOKC";

var MODELS = [];
var accData = {};
try { accData = JSON.parse(localStorage.getItem("acc_"+STATION) || "{}"); } catch(e){}
if(Object.keys(accData).length) MODELS = Object.keys(accData).filter(function(m){ return m !== "NWS"; });
var countdown = 300;
var countdownTimer;

(function(){
  var container = document.getElementById("station-btns");
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
  STATION_LIST.forEach(function(s){
    var btn = document.getElementById("btn-"+s);
    if(!btn) return;
    btn.className = "stn-btn " + (s === STATION ? "active" : "inactive");
  });
}

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

// --- DEFAULT FALLBACK FORM ---
function buildDefaultForm(){
  var mods = MODELS.length ? MODELS : ["HRRR","ARPEGE","NAM","UKMO","LAV-MOS","RAP","GEM-GDPS","NAM-MOS","NBM","NAM4KM"];
  var tbody = document.getElementById("default-tbody");
  if(!tbody) return;
  tbody.innerHTML = mods.map(function(m,i){
    var a = accData[m]||{};
    var rd = (a.runs||{})["default"]||{};
    var bg = i%2?"background:#0a1018":"";
    var namedRuns = Object.keys(a.runs||{}).filter(function(r){ return r!=="default"; }).join(", ")||"none";
    return '<tr style="'+bg+'">'
      +'<td style="color:#e8f0f8;font-weight:600">'+m+'</td>'
      +'<td class="default-col"><input type="number" step="0.1" placeholder="e.g. 1.5" style="width:80px" id="def-mae-'+m+'" value="'+(rd.mae||"")+'"></td>'
      +'<td class="default-col"><input type="number" step="0.1" placeholder="e.g. +0.5" style="width:80px" id="def-corr-'+m+'" value="'+(rd.correction||"")+'"></td>'
      +'<td style="color:var(--dim);font-size:11px">'+namedRuns+'</td>'
      +'</tr>';
  }).join("");
}

function fillDefaultsFromLoaded(){
  var status = document.getElementById("paste-status");
  var filled = 0;
  Object.keys(accData).forEach(function(model){
    if(model === "NWS") return;
    var a = accData[model] || {};
    var mae = parseFloat(a.mae);
    var corr = parseFloat(a.correction);
    if(isNaN(mae) && isNaN(corr)) return;
    var maeEl = document.getElementById("def-mae-"+model);
    var corrEl = document.getElementById("def-corr-"+model);
    if(maeEl && !isNaN(mae)) maeEl.value = mae;
    if(corrEl && !isNaN(corr)) corrEl.value = corr;
    if(maeEl || corrEl) filled++;
  });
  status.style.color = filled > 0 ? "var(--green)" : "var(--red)";
  status.textContent = filled + " models filled from loaded accuracy — hit Save Defaults to commit.";
}

function parseAndFillDefaults(){
  var raw = (document.getElementById("paste-defaults-input").value || "").trim();
  var status = document.getElementById("paste-status");
  if(!raw){ status.textContent = "Nothing to parse."; return; }

  // Model name aliases — wethr sometimes uses different names than our internal keys
  var aliases = {
    "NBS-MOS": "NBS-MOS", "NBSMOS": "NBS-MOS",
    "GFS-MOS": "GFS-MOS", "GFSMOS": "GFS-MOS",
    "LAV-MOS": "LAV-MOS", "LAVMOS": "LAV-MOS",
    "NAM-MOS": "NAM-MOS", "NAMMOS": "NAM-MOS",
    "GEM-GDPS": "GEM-GDPS", "GEMGDPS": "GEM-GDPS",
    "GEM-HRDPS": "GEM-HRDPS", "GEMHRDPS": "GEM-HRDPS",
    "ECMWF-IFS": "ECMWF-IFS", "ECMWFIFS": "ECMWF-IFS",
    "ECMWF-HRES": "ECMWF-HRES", "ECMWFHRES": "ECMWF-HRES",
  };

  var filled = 0, skipped = 0;
  var lines = raw.split("\n");
  lines.forEach(function(line){
    line = line.trim();
    if(!line || line.toLowerCase().startsWith("model")) return; // skip header
    // Split on tabs or multiple spaces
    var cols = line.split(/\t+|\s{2,}/);
    if(cols.length < 3) return;
    var rawModel = cols[0].trim().toUpperCase();
    var model = aliases[rawModel] || rawModel;
    // Parse MAE: strip °, spaces
    var maeRaw = cols[1].replace(/[°\s]/g, "");
    var mae = parseFloat(maeRaw);
    // Parse correction: strip °F, spaces, keep sign
    var corrRaw = cols[2].replace(/[°F\s]/g, "");
    var corr = parseFloat(corrRaw);
    if(isNaN(mae) || isNaN(corr)){ skipped++; return; }
    // Find matching input fields (model must exist in current accData or we create it)
    var maeEl = document.getElementById("def-mae-"+model);
    var corrEl = document.getElementById("def-corr-"+model);
    if(maeEl && corrEl){
      maeEl.value = mae;
      corrEl.value = corr;
      filled++;
    } else {
      // Model not in current table — add to accData anyway so it's saved
      if(!accData[model]) accData[model] = {runs:{}};
      if(!accData[model].runs) accData[model].runs = {};
      accData[model].runs["default"] = {mae: mae, correction: corr};
      filled++;
    }
  });
  status.style.color = filled > 0 ? "var(--green)" : "var(--red)";
  status.textContent = filled + " models filled" + (skipped ? ", " + skipped + " skipped" : "") + " — hit Save Defaults to commit.";
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
  localStorage.setItem("acc_"+STATION, JSON.stringify(accData));
  fetch("/api/accuracy?station="+STATION,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(accData)})
    .then(function(r){ return r.json(); })
    .then(function(){
      status.style.color="var(--green)";
      status.textContent = "Defaults saved at "+new Date().toLocaleTimeString();
      renderPreview();
    }).catch(function(e){
      localStorage.setItem("acc_"+STATION, JSON.stringify(accData));
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
  localStorage.setItem("acc_"+STATION, JSON.stringify(accData));
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
    localStorage.setItem("acc_"+STATION, JSON.stringify(parsed));
    localStorage.setItem("acc_"+STATION+"_time", new Date().toLocaleString());
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
  localStorage.setItem("acc_"+STATION, JSON.stringify(data));
  fetch("/api/accuracy?station="+STATION,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(data)})
    .then(function(){ document.getElementById("save-status").textContent="Saved "+new Date().toLocaleTimeString(); });
}

function clearAccuracy(){
  if(!confirm("Clear all accuracy data?")) return;
  accData = {}; MODELS = [];
  localStorage.removeItem("acc_"+STATION); localStorage.removeItem("acc_"+STATION+"_time");
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
  var t = localStorage.getItem("acc_"+STATION+"_time");
  if(t) document.getElementById("acc-loaded-time").textContent="Loaded: "+t;
  var mods = Object.keys(accData);
  document.getElementById("prev-tbody").innerHTML = mods.map(function(m,i){
    var a = accData[m]||{};
    var defRd = (a.runs||{})["default"]||{};
    var namedRuns = Object.keys(a.runs||{}).filter(function(r){ return r!=="default"; }).filter(function(r){ return (a.runs||{})[r].mae||(a.runs||{})[r].correction; }).join(", ")||"--";
    var bg = i%2?"background:#0a1018":"";
    return '<tr style="'+bg+'">'
      +'<td style="color:#e8f0f8;font-weight:600">'+m+'</td>'
      +'<td style="color:'+maeColor(a.mae)+'">'+(a.mae?fmt1(a.mae)+"F":"--")+'</td>'
      +'<td style="color:'+corrColor(a.correction)+'">'+(a.correction!=null&&a.correction!==""?fmtC(a.correction):"--")+'</td>'
      +'<td style="color:var(--dim)">'+(a.rmse?fmt1(a.rmse)+"F":"--")+'</td>'
      +'<td style="color:'+(defRd.mae?maeColor(defRd.mae):"var(--dimmer)")+'">'+(defRd.mae?fmt1(defRd.mae)+"F":'<span style="color:#2a3a50">—</span>')+'</td>'
      +'<td style="color:'+(defRd.correction!=null&&defRd.correction!==""?corrColor(defRd.correction):"var(--dimmer)")+'">'+(defRd.correction!=null&&defRd.correction!==""?fmtC(defRd.correction):'<span style="color:#2a3a50">—</span>')+'</td>'
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

  document.getElementById("main-tbody").innerHTML = rows.map(function(r,i){
    var bg = i%2?"background:#0a1018":"";
    // Correction source badge
    var corrBadge = "";
    if(r.corr_source === "run") corrBadge = ' <span style="font-size:9px;color:#38bdf8" title="Run-specific correction">R</span>';
    else if(r.corr_source === "default") corrBadge = ' <span style="font-size:9px;color:var(--orange);font-weight:700" title="Using default fallback">D</span>';
    return '<tr style="'+bg+'">'
      +'<td style="color:var(--dim)">#'+r.rank+'</td>'
      +'<td style="color:#e8f0f8;font-weight:600">'+r.model+'</td>'
      +'<td style="color:var(--dim);font-size:11px">'+(r.run||"--")+'</td>'
      +'<td style="color:var(--yellow)">'+(r.raw_high!=null?r.raw_high+"F":"--")+'</td>'
      +'<td style="color:'+corrColor(r.correction)+'">'+(r.correction!=null&&r.correction!==""?fmtC(r.correction)+corrBadge:"--")+'</td>'
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
        +'<div style="width:160px"><div class="pbar" style="width:'+w+'px;background:'+col+'33;border:1px solid '+col+'"></div></div>'
        +'<span style="font-size:11px;color:'+paceColor(r.pace)+';font-weight:600">'+(p>=0?"+":"")+r.pace+'F</span></div>';
    }).join("");
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
    var usingDefault = !hasC && (r.runs||{})["default"] && ((r.runs||{})["default"].correction!=null&&(r.runs||{})["default"].correction!=="");
    var defRd = (r.runs||{})["default"]||{};
    return '<div style="background:#0b1520;border:1px solid '+(usingDefault?"var(--orange)":"var(--border)")+';border-radius:5px;padding:8px 12px;min-width:120px">'
      +'<div style="font-size:11px;color:#8aabcc;font-weight:600">'+r.model+'</div>'
      +'<div style="font-size:13px;color:var(--blue);margin-top:2px">'+(r.run||"--")+'</div>'
      +(hasC?'<div style="font-size:11px;color:'+corrColor(rd.correction)+';margin-top:2px">Corr: '+fmtC(rd.correction)+' <span style="font-size:9px;color:#38bdf8">R</span></div>'
        :usingDefault?'<div style="font-size:11px;color:var(--orange);margin-top:2px">Default: '+fmtC(defRd.correction)+' <span style="font-size:9px">D</span></div>'
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

  document.getElementById("sdot").className = "dot "+(data.errors&&data.errors.length?"dot-yellow":"dot-green");
  document.getElementById("stxt").textContent = data.last_updated?"Updated "+data.last_updated.slice(11,16):"Live";

  // --- Nowcast rendering (appended after existing render code) ---
  try {
    var nc = data.nowcast;
    var ncPill = document.getElementById("h-nowcast-pill");
    var ncSc   = document.getElementById("s-nowcast-sc");
    if(nc && nc.nowcast != null){
      var ncVal = nc.nowcast + "F";
      var ncSub = "+" + nc.sky_boost + "F solar" + (nc.suppressed ? " (wind suppressed)" : "");
      var elHN = document.getElementById("h-nowcast"); if(elHN) elHN.textContent = ncVal;
      var elHS = document.getElementById("h-nowcast-sub"); if(elHS) elHS.textContent = ncSub;
      var elSN = document.getElementById("s-nowcast"); if(elSN) elSN.textContent = ncVal;
      var elSS = document.getElementById("s-nowcast-sub"); if(elSS) elSS.textContent = ncSub;
      if(ncPill) ncPill.style.display = "block";
      if(ncSc)   ncSc.style.display   = "block";
    } else {
      if(ncPill) ncPill.style.display = "none";
      if(ncSc)   ncSc.style.display   = "none";
    }
  } catch(e){ console.error("Nowcast render error", e); }

  // --- Conditions card rendering (appended after existing render code) ---
  try {
    var cond = data.conditions || {};
    var metar = cond.metar || {};
    var condCard = document.getElementById("conditions-card");
    var hasMeta = metar && (metar.flight_category || metar.sky_oktas != null || metar.wind_dir);
    if(hasMeta || cond.solar_noon_obs != null){
      if(condCard) condCard.style.display = "block";
      var elFlt = document.getElementById("cond-fltcat");
      if(elFlt){
        elFlt.textContent = metar.flight_category || "--";
        var fltColors = {VFR:"var(--green)", MVFR:"var(--blue)", IFR:"var(--red)", LIFR:"var(--purple)"};
        elFlt.style.color = fltColors[metar.flight_category] || "var(--text)";
      }
      var elSky = document.getElementById("cond-sky");
      if(elSky) elSky.textContent = metar.sky_oktas != null ? metar.sky_oktas + " oktas" : "--";
      var elWind = document.getElementById("cond-wind");
      if(elWind) elWind.textContent = metar.wind_dir || "--";
      var elKt = document.getElementById("cond-wind-kt");
      if(elKt) elKt.textContent = metar.wind_speed_kt != null ? metar.wind_speed_kt + " kt" : "-- kt";
      var elNoonObs = document.getElementById("cond-noon-obs");
      if(elNoonObs) elNoonObs.textContent = cond.solar_noon_obs != null ? cond.solar_noon_obs + "F" : "--";
      var elNoonDt = document.getElementById("cond-noon-dt");
      if(elNoonDt) elNoonDt.textContent = cond.solar_noon_dt || "--";
      var elRaw = document.getElementById("cond-metar-raw");
      if(elRaw) elRaw.textContent = metar.raw || "";
    } else {
      if(condCard) condCard.style.display = "none";
    }
  } catch(e){ console.error("Conditions render error", e); }

  // --- Today's High Projection rendering ---
  try {
    var thp = data.today_high_projection;
    var thpCard = document.getElementById("thp-card");
    var methodLabels = {
      "decay_integration_regression": "decay fit (multi-pt)",
      "decay_integration_2pt": "decay fit (2-pt)",
      "linear_bridge_capped": "still accelerating — capped bridge",
      "plateau_unconfirmed": "brief plateau — unconfirmed, cautious bridge",
      "already_peaked": "rate stopped — obs is the high",
      "past_peak_window": "past typical peak window"
    };
    if(thp && thp.projected_high != null){
      if(thpCard) thpCard.style.display = "block";
      var elV = document.getElementById("thp-value");
      if(elV) elV.textContent = thp.projected_high + "F";
      var elM = document.getElementById("thp-method");
      if(elM) elM.textContent = methodLabels[thp.method] || thp.method || "--";
      var elRL = document.getElementById("thp-rate-latest");
      if(elRL) elRL.textContent = thp.rate_latest != null ? (thp.rate_latest>=0?"+":"")+thp.rate_latest : "--";
      var elRP = document.getElementById("thp-rate-prev");
      if(elRP) elRP.textContent = thp.rate_prev != null ? (thp.rate_prev>=0?"+":"")+thp.rate_prev : "--";
      var elPT = document.getElementById("thp-peak-time");
      if(elPT) elPT.textContent = thp.peak_time_local || "--";
      var elRem = document.getElementById("thp-remaining");
      if(elRem) elRem.textContent = thp.remaining_hours != null ? thp.remaining_hours + "h left" : "--";
      var elSamp = document.getElementById("thp-samples");
      if(elSamp && thp.samples_used){
        elSamp.textContent = "Samples: " + thp.samples_used.map(function(s){ return s.time+"="+s.temp+"F"; }).join("  ");
      }
    } else if(thp && thp.error){
      if(thpCard) thpCard.style.display = "block";
      var elV2 = document.getElementById("thp-value");
      if(elV2) elV2.textContent = "--";
      var elM2 = document.getElementById("thp-method");
      if(elM2) elM2.textContent = thp.error === "insufficient_data"
        ? ("collecting obs (" + (thp.samples||0) + "/" + (thp.need||3) + ")")
        : thp.error;
      ["thp-rate-latest","thp-rate-prev","thp-peak-time","thp-remaining"].forEach(function(id){
        var el = document.getElementById(id); if(el) el.textContent = "--";
      });
      var elSamp2 = document.getElementById("thp-samples");
      if(elSamp2) elSamp2.textContent = "";
    } else {
      if(thpCard) thpCard.style.display = "none";
    }
  } catch(e){ console.error("Today's High Projection render error", e); }
}

function poll(){
  try { accData = JSON.parse(localStorage.getItem("acc_"+STATION) || "{}"); } catch(e){ accData = {}; }
  if(Object.keys(accData).length){
    fetch("/api/accuracy?station="+STATION,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(accData)});
  }
  fetch("/api/state?station="+STATION).then(function(r){ return r.json(); }).then(render).catch(function(e){ console.error(e); });
}

function manualRefresh(){
  fetch("/api/refresh?station="+STATION,{method:"POST"});
  countdown=300;
  document.getElementById("stxt").textContent="Fetching...";
  setTimeout(poll,5000);
  setTimeout(poll,20000);
  setTimeout(poll,40000);
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

document.getElementById("page-title").textContent = STATION + " \u00b7 Model Tracker";
document.getElementById("page-sub").textContent = STATION_NAMES[STATION] || STATION;

buildForms(); buildDefaultForm(); renderPreview(); poll(); startCountdown(); setInterval(poll,300000);

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

def load_accuracy(station):
    data = load_json_file(f"{DATA_DIR}/accuracy_{station}.json", {})
    if data:
        get_state(station)["accuracy"] = data

def start_background():
    global _started
    with _start_lock:
        if not _started:
            _started = True
            for station in STATIONS:
                load_accuracy(station)
            t = threading.Thread(target=background_loop, daemon=True, name="bgloop")
            t.start()
            print("Background loop started")

with app.app_context():
    start_background()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
