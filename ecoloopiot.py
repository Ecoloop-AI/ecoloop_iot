import dash
from dash import html, dcc, Input, Output, State, dash_table, no_update
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
import requests
import datetime
import hashlib
import random
import threading

# =====================================================
# REMOTE API CONFIG — your cPanel PHP endpoints
# =====================================================
API_BASE = "http://ecoloop.in/api"   # ← CHANGE to your cPanel URL (no trailing slash)
SAVE_URL   = f"{API_BASE}/save_data.php"
GET_URL    = f"{API_BASE}/get_data.php"
LOGIN_URL  = f"{API_BASE}/login.php"
SIGNUP_URL = f"{API_BASE}/signup.php"

# =====================================================
# LOCAL SQLITE FALLBACK — used when cPanel unreachable
# =====================================================

# ── Connectivity state ────────────────────────────────────────────────
_api_ok      = False
_api_checked = 0.0
_API_TTL     = 30
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
}
def _check_api():
    global _api_ok, _api_checked
    now = datetime.datetime.now().timestamp()
    if now - _api_checked < _API_TTL:
        return _api_ok
    try:
        r = requests.get(GET_URL, params={"limit": 1}, headers=HEADERS, timeout=5)
        _api_ok = (r.status_code == 200)
    except Exception:
        _api_ok = False

    _api_checked = now
    status = "✓ ONLINE" if _api_ok else "✗ OFFLINE — using local SQLite fallback"
    print(f"[API] ecoloop.in {status}")
    return _api_ok

def _normalize_df(df):
    df.rename(columns={"created_at": "timestamp", "temp": "temperature", "hum": "humidity"},
              inplace=True)
    for col in ["temperature", "humidity"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce") \
                            .dt.strftime("%Y-%m-%d %H:%M:%S")
    return df

# =====================================================
# DATA HELPERS
# =====================================================
def fetch_sensor_data(limit=200, from_dt=None, to_dt=None):
    try:
        params = {"limit": limit}
        if from_dt: params["from"] = from_dt
        if to_dt:   params["to"]   = to_dt
        
        # We now use the HEADERS to bypass cPanel security
        r = requests.get(GET_URL, params=params, headers=HEADERS, timeout=10)
        data = r.json()
        
        if isinstance(data, list):
            return _normalize_df(pd.DataFrame(data)) if data else pd.DataFrame()
    except Exception as e:
        print(f"API Fetch Error: {e}")
        return pd.DataFrame() # Return empty if cPanel fails
    return pd.DataFrame()

def push_sensor_data(device, temp, hum):
    try:
        # Use 'data=' instead of 'json=' so PHP can read it via $_POST
        payload = {"device": device, "temp": temp, "hum": hum}
        requests.post(SAVE_URL, data=payload, headers=HEADERS, timeout=10)
    except Exception as e:
        print(f"API Push Error: {e}")

def remote_login(username, password):
    enc = hashlib.sha256(password.encode()).hexdigest()
    if _check_api():
        try:
            r = requests.post(LOGIN_URL, data={"username": username, "password": enc}, headers=HEADERS, timeout=6)
            res = r.json()
            if res.get("status") == "ok":
                return res.get("role", "Viewer")
            return None
        except Exception:
            pass

    

def remote_signup(fullname, email, username, password):
    enc = hashlib.sha256(password.encode()).hexdigest()
    if _check_api():
        try:
            r = requests.post(SIGNUP_URL,
                              data={"fullname": fullname, "email": email,
                                    "username": username, "password": enc},
                              headers=HEADERS, timeout=6)
            return r.json()
        except Exception:
            pass

    

# =====================================================
# LIVE SENSOR STATE
# =====================================================
sensor_data = {"temperature": None, "humidity": None}
trend_store = {"timestamps": [], "temps": [], "hums": []}

_last_received  = 0.0
OFFLINE_TIMEOUT = 20  

def is_online(df):
    """
    Checks if the latest record in the dataframe is newer 
    than 20 seconds ago.
    """
    if df.empty or "timestamp" not in df.columns:
        return False
    
    # Get the latest timestamp from the data we just fetched
    latest_str = df["timestamp"].iloc[-1]
    try:
        latest_dt = datetime.datetime.strptime(latest_str, "%Y-%m-%d %H:%M:%S")
        now = datetime.datetime.now()
        
        # Calculate difference in seconds
        diff = (now - latest_dt).total_seconds()
        return diff < OFFLINE_TIMEOUT
    except:
        return False

# =====================================================
# MQTT (optional)
# =====================================================
try:
    import paho.mqtt.client as mqtt

    def on_connect(client, userdata, flags, rc):
        client.subscribe("factory/temp")
        client.subscribe("factory/humidity")

    def on_message(client, userdata, msg):
        try:
            val = float(msg.payload.decode())
            if msg.topic == "factory/temp":
                sensor_data["temperature"] = val
                push_sensor_data("mqtt_temp_sensor", val, sensor_data["humidity"])
            elif msg.topic == "factory/humidity":
                sensor_data["humidity"] = val
                push_sensor_data("mqtt_hum_sensor", sensor_data["temperature"], val)
        except Exception:
            pass

    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    def _connect_mqtt():
        try:
            mqtt_client.connect("broker.hivemq.com", 1883, 60)
            mqtt_client.loop_start()
        except Exception:
            pass

    threading.Thread(target=_connect_mqtt, daemon=True).start()
except ImportError:
    pass

# =====================================================
# APP
# =====================================================
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True
)
server = app.server

from flask import request, jsonify

@server.route("/api/ingest", methods=["POST"])
def ingest():
    global _last_received
    try:
        body   = request.get_json(force=True) or {}
        device = str(body.get("device", "machine1"))
        temp   = float(body.get("temp", 0))
        hum    = float(body.get("hum",  0))

        # ── FIX: update timestamp so sidebar knows data arrived ──────
        _last_received = datetime.datetime.now().timestamp()

        sensor_data["temperature"] = round(temp, 1)
        sensor_data["humidity"]    = round(hum,  1)

        ts = datetime.datetime.now().strftime("%H:%M:%S")
        trend_store["timestamps"].append(ts)
        trend_store["temps"].append(round(temp, 1))
        trend_store["hums"].append(round(hum,  1))
        for k in trend_store:
            trend_store[k] = trend_store[k][-30:]

        push_sensor_data(device, temp, hum)
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)}), 400

app.index_string = '''<!DOCTYPE html>
<html>
<head>
{%metas%}
<title>IIoT Monitor</title>
{%favicon%}
{%css%}
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#f5f6fa;font-family:"Segoe UI",sans-serif;color:#1a1a2e}
.sidebar{width:200px;min-width:200px;background:#fff;border-right:1px solid #e8eaf0;min-height:100vh;display:flex;flex-direction:column;position:fixed;top:0;left:0;bottom:0;z-index:100;overflow-y:auto}
.sidebar-brand{padding:20px 18px 16px;border-bottom:1px solid #f0f1f6}
.brand-row{display:flex;align-items:center;gap:8px}
.brand-dot{width:9px;height:9px;border-radius:50%;background:#e53935;flex-shrink:0}
.brand-title{font-size:14px;font-weight:700;color:#1a1a2e}
.brand-sub{font-size:11px;color:#aab;margin-top:3px;margin-left:17px}
nav{margin-top:10px}
.nav-item{display:flex;align-items:center;gap:10px;padding:10px 18px;font-size:13px;color:#888;cursor:pointer;border-left:3px solid transparent;text-decoration:none;transition:all .15s}
.nav-item:hover{color:#1a1a2e;background:#f7f8fc;text-decoration:none}
.nav-item.active{color:#e53935;border-left:3px solid #e53935;background:#fff5f5;font-weight:600}
.sidebar-foot{padding:14px 18px;border-top:1px solid #f0f1f6;margin-top:auto}
.foot-email{font-size:11px;color:#bbb;margin-top:4px}
.main-wrap{margin-left:200px;min-height:100vh;display:flex;flex-direction:column}
.topbar{background:#fff;border-bottom:1px solid #e8eaf0;padding:14px 28px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:50}
.topbar-title{font-size:16px;font-weight:700;color:#1a1a2e}
.topbar-time{font-size:12px;color:#888;background:#f5f6fa;border:1px solid #e8eaf0;border-radius:20px;padding:4px 14px}
.page-body{padding:24px 28px;flex:1}
.metric-row{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:20px}
.mcard{background:#fff;border:1px solid #e8eaf0;border-radius:12px;padding:16px 18px}
.mcard-label{font-size:10px;color:#aab;text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px}
.mcard-val{font-size:26px;font-weight:700;color:#1a1a2e;line-height:1}
.mcard-unit{font-size:13px;color:#aab;font-weight:400;margin-left:3px}
.mcard-sub{font-size:11px;margin-top:5px}
.sub-ok{color:#43a047}.sub-warn{color:#fb8c00}.sub-danger{color:#e53935}
.gauge-row{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:20px}
.gcard{background:#fff;border:1px solid #e8eaf0;border-radius:12px;padding:18px 20px;position:relative}
.gcard-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:4px}
.gcard-title{font-size:13px;font-weight:600;color:#555}
.gcard-foot{font-size:11px;color:#ccc;text-align:center;margin-top:2px}
.badge-ok{background:#e8f5e9;color:#2e7d32;font-size:11px;padding:3px 10px;border-radius:20px;font-weight:600}
.badge-warn{background:#fff3e0;color:#e65100;font-size:11px;padding:3px 10px;border-radius:20px;font-weight:600}
.badge-danger{background:#ffebee;color:#c62828;font-size:11px;padding:3px 10px;border-radius:20px;font-weight:600}
.chart-card {    background: #fff;    border: 1px solid #e8eaf0;    border-radius: 12px;    padding: 18px 20px;    margin-bottom: 18px;   width: 100%;  display: flex; flex-direction: column;}
.chart-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}
.chart-title{font-size:13px;font-weight:600;color:#555}
.legend{display:flex;gap:16px;align-items:center;font-size:12px;color:#888}
.leg-dot{width:9px;height:9px;border-radius:50%;display:inline-block;margin-right:4px}
.filter-bar{background:#fff;border:1px solid #e8eaf0;border-radius:12px;padding:16px 20px;margin-bottom:16px;display:flex;align-items:flex-end;gap:14px;flex-wrap:wrap}
.filter-group{display:flex;flex-direction:column}
.filter-label{font-size:11px;color:#aab;text-transform:uppercase;letter-spacing:.05em;margin-bottom:5px}
.filter-input{border:1px solid #e0e2ea;border-radius:8px;padding:8px 12px;font-size:13px;color:#1a1a2e;background:#fafbfd;outline:none}
.filter-input:focus{border-color:#e53935}
.btn-search{background:#e53935;color:#fff;border:none;border-radius:8px;padding:9px 22px;font-size:13px;font-weight:600;cursor:pointer}
.btn-search:hover{background:#c62828}
.btn-reset{background:#f5f6fa;color:#888;border:1px solid #e0e2ea;border-radius:8px;padding:9px 16px;font-size:13px;cursor:pointer}
.DateInput_input{font-size:13px!important;color:#1a1a2e!important;padding:8px 10px!important;background:transparent!important;font-family:"Segoe UI",sans-serif!important;border-bottom:2px solid transparent!important}
.DateInput_input__focused{border-bottom:2px solid #e53935!important}
.CalendarDay__selected,.CalendarDay__selected:hover{background:#e53935!important;border-color:#e53935!important;color:#fff!important}
.table-card{background:#fff;border:1px solid #e8eaf0;border-radius:12px;overflow:hidden}
.report-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:18px}
.rcard{background:#fff;border:1px solid #e8eaf0;border-radius:12px;padding:16px 18px}
.rcard-icon{font-size:20px;margin-bottom:8px}
.rcard-val{font-size:22px;font-weight:700;color:#1a1a2e}
.rcard-label{font-size:12px;color:#aab;margin-top:4px}
.dl-row{background:#fff;border:1px solid #e8eaf0;border-radius:12px;padding:16px 20px;display:flex;align-items:center;justify-content:space-between;margin-bottom:10px}
.btn-dl{background:#fff;color:#e53935;border:1px solid #e53935;border-radius:8px;padding:8px 18px;font-size:13px;font-weight:600;cursor:pointer;margin-right:8px}
.btn-dl:hover{background:#fff5f5}
.skeleton{background:linear-gradient(90deg,#f0f1f6 25%,#e8eaf0 50%,#f0f1f6 75%);background-size:200% 100%;animation:shimmer 1.2s infinite;border-radius:12px;height:88px}
@keyframes shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}

/* ── Sidebar status pill ── */
.status-pill{
  display:flex;align-items:center;gap:6px;
  padding:7px 10px;border-radius:8px;
  margin-bottom:8px;
  transition:background .3s;
}
.status-pill.live{background:#e8f5e9;}
.status-pill.offline{background:#ffebee;}
.status-dot{
  width:8px;height:8px;border-radius:50%;flex-shrink:0;
  transition:background .3s;
}
.status-pill.live   .status-dot{background:#43a047;box-shadow:0 0 0 3px rgba(67,160,71,0.25);}
.status-pill.offline .status-dot{background:#e53935;box-shadow:0 0 0 3px rgba(229,57,53,0.20);}
.status-text{font-size:11px;font-weight:700;line-height:1.2;}
.status-pill.live   .status-text{color:#2e7d32;}
.status-pill.offline .status-text{color:#c62828;}
.status-sub{font-size:10px;color:#aab;margin-top:1px;}

.login-wrap{max-width:380px;margin:80px auto;padding:0 16px}
.login-card{background:#fff;border:1px solid #e8eaf0;border-radius:16px;padding:32px}
.login-logo{display:flex;align-items:center;gap:8px;margin-bottom:6px}
.login-title{font-size:20px;font-weight:700;color:#1a1a2e;margin:18px 0 4px}
.login-sub{font-size:13px;color:#aab;margin-bottom:22px}
.form-input{width:100%;border:1px solid #e0e2ea;border-radius:8px;padding:10px 14px;font-size:14px;color:#1a1a2e;background:#fafbfd;margin-bottom:12px;outline:none;display:block}
.form-input:focus{border-color:#e53935}
.btn-login{width:100%;background:#e53935;color:#fff;border:none;border-radius:8px;padding:11px;font-size:14px;font-weight:600;cursor:pointer;margin-bottom:10px;display:block}
.btn-secondary-outline{width:100%;background:#fff;border:1px solid #e0e2ea;border-radius:8px;padding:11px;font-size:14px;cursor:pointer;text-align:center;display:block;text-decoration:none;color:#888}
.btn-secondary-outline:hover{background:#f5f6fa;text-decoration:none;color:#555}
.api-info{background:#f0f7ff;border:1px solid #bbdefb;border-radius:8px;padding:10px 14px;font-size:12px;color:#1565c0;margin-bottom:14px}
</style>
</head>
<body>{%app_entry%}
<footer>{%config%}{%scripts%}{%renderer%}</footer>
</body>
</html>'''

# =====================================================
# LOGIN / SIGNUP
# =====================================================
login_layout = html.Div([
    html.Div([
        html.Div([
            html.Span(className="brand-dot"),
            html.Span("IIoT Monitor", style={"fontSize":"16px","fontWeight":"700","color":"#1a1a2e"})
        ], className="login-logo"),
        html.P("Factory Floor v2.1 · MySQL Cloud",
               style={"fontSize":"12px","color":"#bbb","marginBottom":"0"}),
        html.H4("Welcome back", className="login-title"),
        html.P("Sign in to your account", className="login-sub"),
        html.Div("Connected to ecoloop.in MySQL", className="api-info"),
        dcc.Input(id="username", placeholder="Username", className="form-input"),
        dcc.Input(id="password", placeholder="Password", type="password", className="form-input"),
        html.Button("Sign in", id="login-btn", className="btn-login"),
        html.A("Create account", href="/signup", className="btn-secondary-outline"),
        html.Div(id="login-error",
                 style={"color":"#e53935","fontSize":"13px","marginTop":"10px","textAlign":"center"})
    ], className="login-card")
], className="login-wrap")

signup_layout = html.Div([
    html.Div([
        html.H4("Create account",
                style={"fontSize":"20px","fontWeight":"700","color":"#1a1a2e","marginBottom":"20px"}),
        html.Div("Account stored in ecoloop.in MySQL", className="api-info"),
        dcc.Input(id="su-name",  placeholder="Full Name", className="form-input"),
        dcc.Input(id="su-email", placeholder="Email",     className="form-input"),
        dcc.Input(id="su-user",  placeholder="Username",  className="form-input"),
        dcc.Input(id="su-pass",  placeholder="Password",  type="password", className="form-input"),
        html.Button("Create Account", id="signup-btn", className="btn-login"),
        html.A("Back to Login", href="/login", className="btn-secondary-outline"),
        html.Div(id="signup-msg",
                 style={"fontSize":"13px","marginTop":"10px","textAlign":"center"})
    ], className="login-card")
], className="login-wrap")

# =====================================================
# SIDEBAR — status div is separate & dynamically updated
# =====================================================
def make_sidebar(active, user_email=""):
    """
    Sidebar WITHOUT the status pill — the pill is rendered
    dynamically by the 'sidebar-status' callback so it updates
    every 5 seconds without a full page rebuild.
    """
    def nav(label, href, key):
        return html.A(label, href=href,
                      className="nav-item active" if active == key else "nav-item")
    return html.Div([
        html.Div([
            html.Div([html.Span(className="brand-dot"),
                      html.Span("IIoT Monitor", className="brand-title")], className="brand-row"),
            html.Div("Factory Floor v2.1", className="brand-sub")
        ], className="sidebar-brand"),
        html.Nav([
            nav("Dashboard", "/dashboard", "dashboard"),
            nav("History",   "/history",   "history"),
            nav("Trend",     "/trend",     "trend"),
            nav("Reports",   "/reports",   "reports"),
        ]),
        html.Div([
            # ── FIX: dedicated div — updated by callback every 5 s ──
            html.Div(id="sidebar-status"),
            html.Div(user_email or "ecoloop.in", className="foot-email"),
            html.A("Logout", href="/login",
                   style={"fontSize":"12px","color":"#e53935","textDecoration":"none",
                          "display":"block","marginTop":"8px"})
        ], className="sidebar-foot")
    ], className="sidebar")

def _status_pill(online: bool):
    if online:
        return html.Div([
            html.Div(className="status-dot"),
            html.Div([
                html.Div("● Live", className="status-text"),
                html.Div("Syncing with MySQL", className="status-sub"),
            ])
        ], className="status-pill live")
    else:
        return html.Div([
            html.Div(className="status-dot"),
            html.Div([
                html.Div("● Offline", className="status-text"),
                html.Div("No data in last 20s", className="status-sub"),
            ])
        ], className="status-pill offline")


def page_wrap(active, title, content, user_email=""):
    return html.Div([
        make_sidebar(active, user_email),
        html.Div([
            html.Div([html.Span(title, className="topbar-title"),
                      html.Span(id="topbar-clock", className="topbar-time")], className="topbar"),
            html.Div(content, className="page-body")
        ], className="main-wrap")
    ])

def skeleton_cards():
    return html.Div([html.Div(className="skeleton") for _ in range(4)],
                    className="metric-row")

# =====================================================
# ROOT LAYOUT
# =====================================================
app.layout = html.Div([
    dcc.Location(id="url", refresh=False),
    dcc.Store(id="user-store", storage_type="session"),
    dcc.Interval(id="interval",  interval=5000, n_intervals=0),
    dcc.Interval(id="clock-int", interval=1000, n_intervals=0),
    html.Div(id="page-layout")
])

# =====================================================
# ROUTING
# =====================================================
@app.callback(Output("page-layout", "children"), Input("url", "pathname"))
def route(path):
    if path in ["/", "/login"]: return login_layout
    if path == "/signup":       return signup_layout
    if path == "/history":      return page_wrap("history",  "History",        history_layout())
    if path == "/trend":        return page_wrap("trend",    "Trend Analysis", trend_layout())
    if path == "/reports":      return page_wrap("reports",  "Reports",        reports_layout())
    return page_wrap("dashboard", "Dashboard", dashboard_layout())

# =====================================================
# CLOCK
# =====================================================
@app.callback(Output("topbar-clock","children"), Input("clock-int","n_intervals"))
def clock(n):
    return datetime.datetime.now().strftime("%I:%M:%S %p")

# =====================================================
# ── FIX: SIDEBAR STATUS — updates every 5 s ──────────
# Reads is_online() which checks _last_received timestamp
# set by the /api/ingest Flask endpoint.
# =====================================================
@app.callback(
    Output("sidebar-status", "children"),
    Input("interval", "n_intervals"),
)
def update_sidebar_status(n):
    # Fetch only the very last row from cPanel
    df = fetch_sensor_data(limit=1)
    
    online = is_online(df)
    return _status_pill(online)

# =====================================================
# AUTH
# =====================================================
@app.callback(
    Output("url","pathname", allow_duplicate=True),
    Output("user-store","data"),
    Output("login-error","children"),
    Input("login-btn","n_clicks"),
    State("username","value"),
    State("password","value"),
    prevent_initial_call=True
)
def login(n, user, pw):
    if not user or not pw:
        return no_update, no_update, "Enter username and password"
    role = remote_login(user, pw)
    if role:
        return "/dashboard", {"user": user, "role": role}, ""
    return no_update, no_update, "Invalid username or password"

@app.callback(
    Output("signup-msg","children"),
    Input("signup-btn","n_clicks"),
    State("su-name","value"), State("su-email","value"),
    State("su-user","value"), State("su-pass","value"),
    prevent_initial_call=True
)
def signup(n, name, email, user, pw):
    if not all([name, email, user, pw]):
        return "Please fill all fields"
    res = remote_signup(name, email, user, pw)
    if res.get("status") == "ok":
        return html.Span("Account created! You can now login.", style={"color":"#43a047"})
    return html.Span(res.get("msg","Username or email already exists."),
                     style={"color":"#e53935"})

# =====================================================
# DASHBOARD
# =====================================================
def dashboard_layout():
    return html.Div([
        html.Div(skeleton_cards(), id="metric-cards"),
        html.Div(id="gauge-row-div"),
        html.Div([
            html.Div([
                html.Span("Live trend — last 30 readings", className="chart-title"),
                html.Div([
                    html.Span([html.Span(className="leg-dot",
                               style={"background":"#e53935","display":"inline-block"}), "Temp °C"]),
                    html.Span([html.Span(className="leg-dot",
                               style={"background":"#1976d2","display":"inline-block"}), "Humidity %"]),
                ], className="legend")
            ], className="chart-head"),
            dcc.Graph(
                id="trend-chart", 
                config={"displayModeBar": False}, 
                style={
                    "height": "350px", 
                    "width": "100%"  # <--- Ensure this is 100%
                }
            )
        ], className="chart-card"),
        html.Div(id="db-status",
                 style={"fontSize":"11px","color":"#aab","textAlign":"right","marginTop":"4px"})
    ])

@app.callback(
    Output("metric-cards",  "children"),
    Output("gauge-row-div", "children"),
    Output("trend-chart",   "figure"),
    Output("db-status",     "children"),
    Input("interval",       "n_intervals")
)
def update_dashboard(n):
    df = fetch_sensor_data(limit=30)

    if df.empty or "temperature" not in df.columns:
        T  = sensor_data["temperature"]
        H  = sensor_data["humidity"]
        ts_list   = []
        temp_list = [T] if T is not None else [0]
        hum_list  = [H] if H is not None else [0]
        db_msg = "⚠ Remote DB unreachable — using local SQLite fallback"
    else:
        df_sorted = df.sort_values("timestamp") if "timestamp" in df.columns else df
        T  = round(float(df_sorted["temperature"].iloc[-1]), 1)
        H  = round(float(df_sorted["humidity"].iloc[-1]),    1)
        ts_list   = df_sorted["timestamp"].tolist()[-30:]
        temp_list = df_sorted["temperature"].tolist()[-30:]
        hum_list  = df_sorted["humidity"].tolist()[-30:]
        db_msg = f"✓ MySQL · ecoloop.in · {len(df)} live rows"

    if T is None: T = 0.0
    if H is None: H = 0.0

    tc = "badge-danger" if T>=35 else "badge-warn" if T>=30 else "badge-ok"
    tl = "ALERT"       if T>=35 else "Warm"        if T>=30 else "Normal"
    hc = "badge-danger" if H>=70 else "badge-warn" if H>=60 else "badge-ok"
    hl = "ALERT"       if H>=70 else "Elevated"    if H>=60 else "Normal"
    t_sub_cls = "sub-danger" if T>=35 else "sub-warn" if T>=30 else "sub-ok"
    h_sub_cls = "sub-danger" if H>=70 else "sub-warn" if H>=60 else "sub-ok"
    t_sub = "▲ Alert" if T>=35 else "▲ High"     if T>=30 else "✓ Normal"
    h_sub = "▲ Alert" if H>=70 else "▲ Elevated" if H>=60 else "✓ Normal"

    avg_t = round(sum(temp_list)/len(temp_list), 1) if temp_list else 0
    avg_h = round(sum(hum_list)/len(hum_list),   1) if hum_list  else 0

    cards = html.Div([
        _mcard("TEMPERATURE",            f"{T}",     "°C", t_sub, t_sub_cls),
        _mcard("HUMIDITY",               f"{H}",     "%",  h_sub, h_sub_cls),
        _mcard("AVG TEMP (30 READINGS)", f"{avg_t}", "°C",
               f"Range {round(min(temp_list),1)}–{round(max(temp_list),1)}°C", "sub-ok"),
        _mcard("AVG HUMIDITY",           f"{avg_h}", "%", "Last 30 readings", "sub-ok"),
    ], className="metric-row")

    t_color = "#e53935" if T>=35 else "#fb8c00" if T>=30 else "#43a047"
    h_color = "#e53935" if H>=70 else "#fb8c00" if H>=60 else "#1976d2"

    gauges = html.Div([
        html.Div([
            html.Div([html.Span("Temperature sensor", className="gcard-title"),
                      html.Span(tl, className=tc)], className="gcard-head"),
            dcc.Graph(figure=_gauge_fig(T, 60, t_color, [
                {"range":[0,30],"color":"#f5f6fa"},{"range":[30,35],"color":"#fff3e0"},
                {"range":[35,60],"color":"#ffebee"}]),
                config={"displayModeBar":False}, style={"height":"190px"}),
            html.P("ecoloop.in/api/get_data.php", className="gcard-foot")
        ], className="gcard"),
        html.Div([
            html.Div([html.Span("Humidity sensor", className="gcard-title"),
                      html.Span(hl, className=hc)], className="gcard-head"),
            dcc.Graph(figure=_gauge_fig(H, 100, h_color, [
                {"range":[0,60],"color":"#f5f6fa"},{"range":[60,70],"color":"#fff3e0"},
                {"range":[70,100],"color":"#ffebee"}]),
                config={"displayModeBar":False}, style={"height":"190px"}),
            html.P("ecoloop.in/api/save_data.php", className="gcard-foot")
        ], className="gcard"),
    ], className="gauge-row")

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=ts_list, y=temp_list,
        name="Temp °C", line=dict(color="#e53935",width=2),
        fill="tozeroy", fillcolor="rgba(229,57,53,0.08)", mode="lines"))
    fig.add_trace(go.Scatter(x=ts_list, y=hum_list,
        name="Humidity %", line=dict(color="#1976d2",width=2,dash="dash"),
        fill="tozeroy", fillcolor="rgba(25,118,210,0.07)", mode="lines"))
    fig.update_layout(
    autosize=True,      # Forces the chart to stretch to the container
    margin=dict(t=10, b=30, l=40, r=20), # Small 'r' (right) margin
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    xaxis=dict(showgrid=False),
    yaxis=dict(showgrid=True, gridcolor="#f0f1f6"),
    showlegend=False
        )

    return cards, gauges, fig, db_msg

# =====================================================
# HISTORY
# =====================================================
def history_layout():
    return html.Div([
        html.Div(id="history-table-container",
                 children=html.P("Loading from MySQL…",
                                 style={"color":"#aab","fontSize":"13px","padding":"20px"})),
        dcc.Interval(id="history-load", interval=300, n_intervals=0, max_intervals=1)
    ])

@app.callback(
    Output("history-table-container","children"),
    Input("history-load","n_intervals"),
    prevent_initial_call=False
)
def load_history(_):
    df = fetch_sensor_data(limit=200)
    if df.empty:
        return html.P("No data returned from MySQL. Check API endpoint.",
                      style={"color":"#e53935","fontSize":"13px","padding":"20px"})

    cols = []
    if "id" in df.columns:         cols.append({"name":"#","id":"id"})
    if "device" in df.columns:     cols.append({"name":"Device","id":"device"})
    if "timestamp" in df.columns:  cols.append({"name":"Timestamp","id":"timestamp"})
    if "temperature" in df.columns: cols.append({"name":"Temp (°C)","id":"temperature"})
    if "humidity" in df.columns:   cols.append({"name":"Humidity (%)","id":"humidity"})

    return [
        html.P(f"Showing {len(df)} most recent readings · ecoloop.in MySQL",
               style={"fontSize":"13px","color":"#aab","marginBottom":"14px"}),
        html.Div(dash_table.DataTable(
            data=df.to_dict("records"),
            columns=cols,
            page_size=20, sort_action="native", filter_action="native",
            style_table={"overflowX":"auto"},
            style_header={"backgroundColor":"#f5f6fa","color":"#aab","fontSize":"11px",
                          "fontWeight":"600","border":"none","borderBottom":"1px solid #e8eaf0",
                          "textTransform":"uppercase","letterSpacing":"0.05em","padding":"10px 14px"},
            style_cell={"backgroundColor":"#fff","color":"#1a1a2e","fontSize":"13px","border":"none",
                        "borderBottom":"1px solid #f0f1f6","padding":"10px 14px",
                        "fontFamily":"Segoe UI, sans-serif"},
            style_data_conditional=[
                {"if":{"filter_query":"{temperature} > 35","column_id":"temperature"},
                 "color":"#c62828","fontWeight":"600"},
                {"if":{"filter_query":"{temperature} > 30 && {temperature} <= 35","column_id":"temperature"},
                 "color":"#e65100"},
                {"if":{"filter_query":"{temperature} <= 30","column_id":"temperature"},
                 "color":"#2e7d32"},
                {"if":{"filter_query":"{humidity} > 70","column_id":"humidity"},
                 "color":"#c62828","fontWeight":"600"},
                {"if":{"filter_query":"{humidity} > 60 && {humidity} <= 70","column_id":"humidity"},
                 "color":"#e65100"},
                {"if":{"filter_query":"{humidity} <= 60","column_id":"humidity"},
                 "color":"#1565c0"},
            ],
        ), className="table-card")
    ]

# =====================================================
# TREND
# =====================================================
def trend_layout():
    today    = datetime.date.today().isoformat()
    week_ago = (datetime.date.today() - datetime.timedelta(days=7)).isoformat()
    return html.Div([
        html.Div([
            html.Div([
                html.Div("From date", className="filter-label"),
                dcc.DatePickerSingle(id="trend-from", date=week_ago,
                    display_format="YYYY-MM-DD", first_day_of_week=1,
                    placeholder="Select date", style={"fontSize":"13px"})
            ], className="filter-group"),
            html.Div([
                html.Div("From time (HH:MM)", className="filter-label"),
                dcc.Input(id="trend-from-time", type="text", value="00:00",
                          placeholder="00:00", className="filter-input", style={"width":"110px"})
            ], className="filter-group"),
            html.Div([
                html.Div("To date", className="filter-label"),
                dcc.DatePickerSingle(id="trend-to", date=today,
                    display_format="YYYY-MM-DD", first_day_of_week=1,
                    placeholder="Select date", style={"fontSize":"13px"})
            ], className="filter-group"),
            html.Div([
                html.Div("To time (HH:MM)", className="filter-label"),
                dcc.Input(id="trend-to-time", type="text", value="23:59",
                          placeholder="23:59", className="filter-input", style={"width":"110px"})
            ], className="filter-group"),
            html.Button("Search", id="trend-search-btn", className="btn-search"),
            html.Button("Reset",  id="trend-reset-btn",  className="btn-reset"),
        ], className="filter-bar"),

        html.Div(id="trend-metrics",
                 children=html.P("Select a date range and click Search.",
                                 style={"color":"#aab","fontSize":"13px"})),
        html.Div([
            html.Div([html.Span("Temperature over time", className="chart-title"),
                      html.Span(id="trend-range-label",
                                style={"fontSize":"12px","color":"#aab"})], className="chart-head"),
            dcc.Graph(id="trend-temp-chart", config={"displayModeBar":True}, style={"height":"240px"})
        ], className="chart-card"),
        html.Div([
            html.Div(html.Span("Humidity over time", className="chart-title"), className="chart-head"),
            dcc.Graph(id="trend-hum-chart", config={"displayModeBar":True}, style={"height":"240px"})
        ], className="chart-card"),
        html.Div([
            html.Div([
                html.Span("Temperature vs Humidity", className="chart-title"),
                html.Div([
                    html.Span([html.Span(className="leg-dot",
                               style={"background":"#e53935","display":"inline-block"}), "Temp °C"]),
                    html.Span([html.Span(className="leg-dot",
                               style={"background":"#1976d2","display":"inline-block"}), "Humidity %"]),
                ], className="legend")
            ], className="chart-head"),
            dcc.Graph(id="trend-combined-chart", config={"displayModeBar":True}, style={"height":"260px"})
        ], className="chart-card"),
    ])


@app.callback(
    Output("trend-metrics",        "children"),
    Output("trend-temp-chart",     "figure"),
    Output("trend-hum-chart",      "figure"),
    Output("trend-combined-chart", "figure"),
    Output("trend-range-label",    "children"),
    Input("trend-search-btn", "n_clicks"),
    Input("trend-reset-btn",  "n_clicks"),
    State("trend-from",       "date"),
    State("trend-to",         "date"),
    State("trend-from-time",  "value"),
    State("trend-to-time",    "value"),
    prevent_initial_call=True
)
def update_trend(_, __, from_d, to_d, from_t, to_t):
    import re
    from dash import ctx
    today    = datetime.date.today().isoformat()
    week_ago = (datetime.date.today() - datetime.timedelta(days=7)).isoformat()

    if ctx.triggered_id == "trend-reset-btn":
        from_d, to_d, from_t, to_t = week_ago, today, "00:00", "23:59"

    fd = (from_d or week_ago)[:10]
    td = (to_d   or today)[:10]
    ft = from_t if re.match(r"^\d{2}:\d{2}$", from_t or "") else "00:00"
    tt = to_t   if re.match(r"^\d{2}:\d{2}$", to_t   or "") else "23:59"

    df = fetch_sensor_data(limit=5000, from_dt=f"{fd} {ft}:00", to_dt=f"{td} {tt}:59")

    empty = _empty_fig("No data for selected range")
    if df.empty or "temperature" not in df.columns:
        return (html.P("No data found. Check date range or API.",
                       style={"color":"#e53935","fontSize":"13px"}),
                empty, empty, empty, f"{fd} {ft} → {td} {tt}")

    df = df.sort_values("timestamp")
    count = len(df)
    avg_t = round(df["temperature"].mean(), 1)
    avg_h = round(df["humidity"].mean(),    1) if "humidity" in df.columns else 0
    max_t = round(df["temperature"].max(),  1)
    min_t = round(df["temperature"].min(),  1)
    max_h = round(df["humidity"].max(),     1) if "humidity" in df.columns else 0
    min_h = round(df["humidity"].min(),     1) if "humidity" in df.columns else 0

    metrics = html.Div([
        _mcard("DATA POINTS",  str(count),  "",   f"{fd} to {td}", "sub-ok"),
        _mcard("AVG TEMP",     str(avg_t),  "°C", f"Min {min_t} / Max {max_t}", "sub-ok"),
        _mcard("AVG HUMIDITY", str(avg_h),  "%",  f"Min {min_h} / Max {max_h}", "sub-ok"),
        _mcard("PEAK TEMP",    str(max_t),  "°C",
               "Alert!" if max_t>35 else "Normal", "sub-danger" if max_t>35 else "sub-ok"),
    ], className="metric-row")

    def blayout(h):
        return dict(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    showlegend=False, height=h, margin=dict(t=8,b=28,l=40,r=14),
                    xaxis=dict(showgrid=False, tickfont=dict(color="#bbb",size=10)),
                    yaxis=dict(showgrid=True, gridcolor="#f0f1f6",
                               tickfont=dict(color="#bbb",size=10)),
                    hovermode="x unified")

    fig_t = go.Figure()
    fig_t.add_trace(go.Scatter(x=df["timestamp"], y=df["temperature"],
        line=dict(color="#e53935",width=2), fill="tozeroy",
        fillcolor="rgba(229,57,53,0.08)", mode="lines"))
    fig_t.add_hline(y=35, line_dash="dot", line_color="#e53935",
        annotation_text="Alert 35°C", annotation_position="top right",
        annotation_font_size=11, annotation_font_color="#e53935")
    fig_t.add_hline(y=30, line_dash="dot", line_color="#fb8c00",
        annotation_text="Warn 30°C", annotation_position="top right",
        annotation_font_size=11, annotation_font_color="#fb8c00")
    fig_t.update_layout(**blayout(230))

    fig_h = go.Figure()
    if "humidity" in df.columns:
        fig_h.add_trace(go.Scatter(x=df["timestamp"], y=df["humidity"],
            line=dict(color="#1976d2",width=2), fill="tozeroy",
            fillcolor="rgba(25,118,210,0.08)", mode="lines"))
        fig_h.add_hline(y=70, line_dash="dot", line_color="#e53935",
            annotation_text="Alert 70%", annotation_position="top right",
            annotation_font_size=11, annotation_font_color="#e53935")
        fig_h.add_hline(y=60, line_dash="dot", line_color="#fb8c00",
            annotation_text="Warn 60%", annotation_position="top right",
            annotation_font_size=11, annotation_font_color="#fb8c00")
    fig_h.update_layout(**blayout(230))

    fig_c = go.Figure()
    fig_c.add_trace(go.Scatter(x=df["timestamp"], y=df["temperature"],
        name="Temp °C", line=dict(color="#e53935",width=2), mode="lines"))
    if "humidity" in df.columns:
        fig_c.add_trace(go.Scatter(x=df["timestamp"], y=df["humidity"],
            name="Humidity %", line=dict(color="#1976d2",width=2,dash="dash"),
            mode="lines", yaxis="y2"))
    fig_c.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False, height=250, margin=dict(t=8,b=28,l=40,r=54),
        hovermode="x unified",
        xaxis=dict(showgrid=False, tickfont=dict(color="#bbb",size=10)),
        yaxis=dict(title="Temp °C", showgrid=True, gridcolor="#f0f1f6",
                   tickfont=dict(color="#e53935",size=10)),
        yaxis2=dict(title="Humidity %", overlaying="y", side="right",
                    tickfont=dict(color="#1976d2",size=10), showgrid=False))

    return metrics, fig_t, fig_h, fig_c, f"{fd} {ft} → {td} {tt}  ({count} readings)"

# =====================================================
# REPORTS
# =====================================================
def reports_layout():
    today    = datetime.date.today().isoformat()
    week_ago = (datetime.date.today() - datetime.timedelta(days=7)).isoformat()
    return html.Div([
        html.Div([
            html.Div([
                html.Div("From date", className="filter-label"),
                dcc.DatePickerSingle(id="rep-from", date=week_ago,
                    display_format="YYYY-MM-DD", first_day_of_week=1,
                    placeholder="Select date", style={"fontSize":"13px"})
            ], className="filter-group"),
            html.Div([
                html.Div("To date", className="filter-label"),
                dcc.DatePickerSingle(id="rep-to", date=today,
                    display_format="YYYY-MM-DD", first_day_of_week=1,
                    placeholder="Select date", style={"fontSize":"13px"})
            ], className="filter-group"),
            html.Button("Search", id="rep-search-btn", className="btn-search"),
            html.Button("Reset",  id="rep-reset-btn",  className="btn-reset"),
        ], className="filter-bar"),
        html.Div(id="report-body",
                 children=html.P("Select a date range and click Search.",
                                 style={"color":"#aab","fontSize":"13px"})),
        dcc.Download(id="download-file"),
        dcc.Download(id="download-json-file"),
        dcc.Store(id="report-range-store", data={"from": week_ago, "to": today}),
    ])

@app.callback(
    Output("report-body","children"),
    Output("report-range-store","data"),
    Input("rep-search-btn","n_clicks"),
    Input("rep-reset-btn", "n_clicks"),
    State("rep-from","date"),
    State("rep-to",  "date"),
    prevent_initial_call=True
)
def update_report(_, __, from_d, to_d):
    from dash import ctx
    today    = datetime.date.today().isoformat()
    week_ago = (datetime.date.today() - datetime.timedelta(days=7)).isoformat()
    if ctx.triggered_id == "rep-reset-btn":
        from_d, to_d = week_ago, today
    fd = (from_d or week_ago)[:10]
    td = (to_d   or today)[:10]

    df = fetch_sensor_data(limit=10000, from_dt=f"{fd} 00:00:00", to_dt=f"{td} 23:59:59")

    count  = len(df)
    avg_t  = round(df["temperature"].mean(), 1) if count and "temperature" in df.columns else 0
    avg_h  = round(df["humidity"].mean(),    1) if count and "humidity"    in df.columns else 0
    max_t  = round(df["temperature"].max(),  1) if count and "temperature" in df.columns else 0
    min_t  = round(df["temperature"].min(),  1) if count and "temperature" in df.columns else 0
    max_h  = round(df["humidity"].max(),     1) if count and "humidity"    in df.columns else 0
    alerts = 0
    if count:
        if "temperature" in df.columns: alerts += int((df["temperature"]>35).sum())
        if "humidity"    in df.columns: alerts += int((df["humidity"]>70).sum())

    body = html.Div([
        html.Div([
            html.Div([html.Div("📊",className="rcard-icon"),
                      html.Div(str(count),className="rcard-val"),
                      html.Div("Total readings",className="rcard-label")],className="rcard"),
            html.Div([html.Div("🌡️",className="rcard-icon"),
                      html.Div([str(avg_t),
                                html.Span("°C",style={"fontSize":"14px","color":"#aab","marginLeft":"3px"})],
                               className="rcard-val"),
                      html.Div(f"Avg temp · {min_t}–{max_t}°C",className="rcard-label")],className="rcard"),
            html.Div([html.Div("💧",className="rcard-icon"),
                      html.Div([str(avg_h),
                                html.Span("%",style={"fontSize":"14px","color":"#aab","marginLeft":"3px"})],
                               className="rcard-val"),
                      html.Div(f"Avg humidity · Max {max_h}%",className="rcard-label")],className="rcard"),
            html.Div([html.Div("🚨",className="rcard-icon"),
                      html.Div(str(alerts),className="rcard-val",
                               style={"color":"#e53935" if alerts>0 else "#1a1a2e"}),
                      html.Div("Threshold breaches",className="rcard-label")],className="rcard"),
        ], className="report-grid"),
        (html.Div([
            html.Span("Temperature distribution",className="chart-title",
                      style={"display":"block","marginBottom":"10px"}),
            dcc.Graph(figure=_hist_fig(df["temperature"].tolist(),"#e53935"),
                      config={"displayModeBar":False},style={"height":"180px"})
        ], className="chart-card") if count and "temperature" in df.columns else html.Div()),
        (html.Div([
            html.Span("Humidity distribution",className="chart-title",
                      style={"display":"block","marginBottom":"10px"}),
            dcc.Graph(figure=_hist_fig(df["humidity"].tolist(),"#1976d2"),
                      config={"displayModeBar":False},style={"height":"180px"})
        ], className="chart-card") if count and "humidity" in df.columns else html.Div()),
        html.Div([
            html.Div([
                html.P(f"sensor_report_{fd}_to_{td}",
                       style={"fontWeight":"600","fontSize":"13px","color":"#1a1a2e","marginBottom":"4px"}),
                html.P(f"{count} rows · {fd} → {td} · MySQL",
                       style={"fontSize":"12px","color":"#aab"})
            ]),
            html.Div([
                html.Button("⬇ Download CSV",  id="download-btn",      className="btn-dl"),
                html.Button("⬇ Download JSON", id="download-json-btn", className="btn-dl"),
            ])
        ], className="dl-row"),
    ])
    return body, {"from": fd, "to": td}

def _hist_fig(values, color):
    fig = go.Figure(go.Histogram(x=values, nbinsx=20, marker_color=color,
        marker_line_color="#fff", marker_line_width=1, opacity=0.8))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False, height=165, margin=dict(t=4,b=24,l=36,r=12),
        xaxis=dict(showgrid=False, tickfont=dict(color="#bbb",size=10)),
        yaxis=dict(showgrid=True, gridcolor="#f0f1f6", tickfont=dict(color="#bbb",size=10)),
        bargap=0.05)
    return fig

# =====================================================
# DOWNLOAD
# =====================================================
@app.callback(
    Output("download-file","data"),
    Input("download-btn","n_clicks"),
    State("report-range-store","data"),
    prevent_initial_call=True
)
def dl_csv(_, rng):
    df = fetch_sensor_data(limit=50000,
                           from_dt=f"{rng['from']} 00:00:00",
                           to_dt=f"{rng['to']} 23:59:59")
    return dcc.send_data_frame(df.to_csv, f"sensor_{rng['from']}_to_{rng['to']}.csv", index=False)

@app.callback(
    Output("download-json-file","data"),
    Input("download-json-btn","n_clicks"),
    State("report-range-store","data"),
    prevent_initial_call=True
)
def dl_json(_, rng):
    df = fetch_sensor_data(limit=50000,
                           from_dt=f"{rng['from']} 00:00:00",
                           to_dt=f"{rng['to']} 23:59:59")
    return dcc.send_data_frame(df.to_json, f"sensor_{rng['from']}_to_{rng['to']}.json",
                               orient="records", date_format="iso")

# =====================================================
# SHARED HELPERS
# =====================================================
def _mcard(label, val, unit, sub, sub_cls):
    return html.Div([
        html.Div(label, className="mcard-label"),
        html.Div([html.Span(val, className="mcard-val"), html.Span(unit, className="mcard-unit")]),
        html.Div(sub, className=f"mcard-sub {sub_cls}")
    ], className="mcard")

def _gauge_fig(val, max_val, color, steps):
    fig = go.Figure(go.Indicator(
        mode="gauge+number", value=val,
        number={"font":{"color":"#1a1a2e","size":30,"family":"Segoe UI"}},
        gauge={"axis":{"range":[0,max_val],"tickfont":{"color":"#ccc","size":10},"tickcolor":"#e0e2ea"},
               "bar":{"color":color,"thickness":0.22},
               "bgcolor":"#fff","borderwidth":0,"steps":steps}))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font={"color":"#1a1a2e"}, height=175, margin=dict(t=10,b=10,l=20,r=20))
    return fig

def _empty_fig(msg="No data"):
    fig = go.Figure()
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        annotations=[{"text":msg,"x":0.5,"y":0.5,"xref":"paper","yref":"paper",
                      "showarrow":False,"font":{"color":"#aab","size":14}}],
        margin=dict(t=10,b=20,l=36,r=10))
    return fig

# =====================================================
# RUN
# =====================================================
if __name__ == "__main__":
    app.run(debug=False)
