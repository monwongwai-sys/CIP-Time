"""
CIP Performance Monitoring & Analytics
=======================================
- Auth: HTTPBasicAuth (same as original working code)
- Added: Parallel fetch, Auto-retry, Disk cache
- No additional libraries required
"""

import streamlit as st
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth   # ← restored to original
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import urllib3
import concurrent.futures
import time
import pickle
import os
import hashlib

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

st.set_page_config(page_title="CIP Monitoring & Analytics Pro", layout="wide")

FACTORY_CONFIG = {
    "PK1": {
        "tags": {
            "R421": "BEB1-10-0400A-TI421", "R422": "BEB1-10-0400A-TI422",
            "R423": "BEB1-10-0400A-TI423", "R424B": "BEB1-10-0400A-TI424B",
            "R424": "BEB1-10-0400A-TI424", "R425": "BEB1-10-0400A-TI425",
            "R426": "BEB1-10-0400A-TI426"
        },
        "cip_tag": None
    },
    "PK2": {
        "tags": {
            "R421": "BEB1-10-0400B-TT421", "R422": "BEB1-10-0400B-TT422",
            "R423": "BEB1-10-0400B-TT423", "R423B": "BEB1-10-0400B-TT423B",
            "R424": "BEB1-10-0400B-TT424", "R425": "BEB1-10-0400B-TT425",
            "R426": "BEB1-10-0400B-TT426"
        },
        "cip_tag": None
    },
    "KN": {
        "tags": {
            "R421": "OEO1-10-0400-TT421", "R422": "OEO1-10-0400-TT422",
            "R423": "OEO1-10-0400-TT423", "R424": "OEO1-10-0400-TT424",
            "R425": "OEO1-10-0400-TT425", "R426": "OEO1-10-0400-TT426",
            "R427": "OEO1-10-0400-TT427"
        },
        "cip_tag": None
    },
    "DC": {
        "tags": {
            "R421": "BEB3-10-0400-TT421", "R422": "BEB3-10-0400-TT422B",
            "R423": "BEB3-10-0400-TT423B", "R424": "BEB3-10-0400-TT424B",
            "R425": "BEB3-10-0400-TT425B", "R426": "BEB3-10-0400-TT426B",
            "R427": "BEB3-10-0400-TT427B"
        },
        "cip_tag": "BEB3-57-0100-CIP"
    },
    "MCE": {
        "tags": {
            "R421": "CEC1-10-0400-TI421", "R422": "CEC1-10-0400-TI422",
            "R423": "CEC1-10-0400-TI423", "R424": "CEC1-10-0400-TI424",
            "R425": "CEC1-10-0400-TI425", "R426": "CEC1-10-0400-TI426"
        },
        "cip_tag": None
    }
}

# Cache stored in same folder as script
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cip_cache")
os.makedirs(CACHE_DIR, exist_ok=True)
CACHE_TTL_HOURS = 6

if "results"      not in st.session_state: st.session_state.results      = {}
if "view_history" not in st.session_state: st.session_state.view_history = None
if "fetch_errors" not in st.session_state: st.session_state.fetch_errors = []

st.markdown("""
    <style>
    header {visibility: hidden;} #MainMenu {visibility: hidden;} footer {visibility: hidden;}
    .tank-card { border-radius:15px; padding:15px; text-align:center; background:#fff;
        box-shadow:0 4px 12px rgba(0,0,0,0.1); margin-bottom:10px; border-top:8px solid #ddd;
        display:flex; flex-direction:column; align-items:center; }
    .status-pass { border-top-color:#28a745; } .status-fail { border-top-color:#dc3545; }
    .metric-box { text-align:left; font-size:0.82em; background:#f8f9fa; padding:10px;
        border-radius:8px; margin-top:10px; line-height:1.6; width:100%; }
    .latest-time { font-size:0.85em; color:#1a73e8; font-weight:bold; margin-bottom:0; }
    .summary-badge { padding:5px 12px; border-radius:20px; font-weight:bold; color:#fff;
        display:inline-block; margin-right:5px; font-size:0.8em; }
    .error-box { background:#fff3cd; border:1px solid #ffc107; border-radius:8px;
        padding:10px; margin:5px 0; font-size:0.85em; }
    </style>""", unsafe_allow_html=True)


# ============================================================
# DISK CACHE
# ============================================================
def _cache_key(tag, date_str):
    return hashlib.md5(f"{tag}_{date_str}".encode()).hexdigest()

def _load_cache(tag, date_str):
    fpath = os.path.join(CACHE_DIR, f"{_cache_key(tag, date_str)}.pkl")
    if not os.path.exists(fpath): return None
    if (time.time() - os.path.getmtime(fpath)) / 3600 > CACHE_TTL_HOURS:
        os.remove(fpath); return None
    try:
        with open(fpath, "rb") as f: return pickle.load(f)
    except: return None

def _save_cache(tag, date_str, df):
    fpath = os.path.join(CACHE_DIR, f"{_cache_key(tag, date_str)}.pkl")
    try:
        with open(fpath, "wb") as f: pickle.dump(df, f)
    except: pass


# ============================================================
# PI FETCH — HTTPBasicAuth (same as original) + retry + cache
# ============================================================
PI_BASE = "https://piazu.mitrphol.com/piwebapi"

def get_data_pi(tag_path, auth, start_time, end_time=None, max_retries=3):
    if not tag_path:
        return pd.DataFrame(columns=['Time', 'Val'])

    date_str  = start_time.strftime("%Y-%m-%d") if hasattr(start_time, 'strftime') else str(start_time)
    end_date_str = end_time.strftime("%Y-%m-%d") if end_time and hasattr(end_time, 'strftime') else "now"
    date_str = f"{date_str}_to_{end_date_str}"
    start_str = start_time.strftime("%Y-%m-%dT00:00:00Z") if hasattr(start_time, 'strftime') else f"{start_time}T00:00:00Z"
    end_str   = end_time.strftime("%Y-%m-%dT23:59:59Z") if end_time and hasattr(end_time, 'strftime') else "*"

    # check cache
    cached = _load_cache(tag_path, date_str)
    if cached is not None:
        return cached

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(
                f"{PI_BASE}/points",
                params={"path": f"\\\\MPAZU-PIDCDB\\{tag_path}"},
                auth=auth, verify=False, timeout=20
            )
            if r.status_code == 401:
                raise ValueError("HTTP 401 — Incorrect Username or Password")
            if r.status_code != 200:
                raise ValueError(f"HTTP {r.status_code}")

            webid = r.json()["WebId"]
            r2 = requests.get(
                f"{PI_BASE}/streams/{webid}/recorded",
                params={"startTime": start_str, "endTime": end_str, "maxCount": 50000},
                auth=auth, verify=False, timeout=45
            )
            items = r2.json().get("Items", [])
            if not items:
                df = pd.DataFrame(columns=['Time', 'Val'])
                _save_cache(tag_path, date_str, df)
                return df

            df = pd.DataFrame(items)
            df['Time'] = (pd.to_datetime(df['Timestamp'], format='ISO8601')
                          .dt.tz_convert('Asia/Bangkok').dt.tz_localize(None))
            df['Val'] = pd.to_numeric(
                df['Value'].apply(lambda x: x.get('Value') if isinstance(x, dict) else x),
                errors='coerce')
            df = df[['Time', 'Val']].dropna().sort_values('Time')
            _save_cache(tag_path, date_str, df)
            return df

        except Exception as e:
            err = str(e)
            if "401" in err:
                # 401 → no retry — invalid credentials
                return pd.DataFrame({'Time':[None],'Val':[None],'_error':[err],'_tag':[tag_path]})
            if attempt < max_retries:
                time.sleep(attempt * 2)
            else:
                return pd.DataFrame({'Time':[None],'Val':[None],'_error':[err],'_tag':[tag_path]})

    return pd.DataFrame(columns=['Time', 'Val'])


def fetch_all_tags_parallel(tag_dict, auth, start_time, end_time=None, max_workers=5):
    """Fetch all tanks in parallel"""
    results, errors = {}, {}

    def _one(item):
        name, tag = item
        return name, get_data_pi(tag, auth, start_time, end_time)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_one, it): it for it in tag_dict.items()}
        for fut in concurrent.futures.as_completed(futs):
            try:
                name, df = fut.result()
                if '_error' in df.columns and not df.empty:
                    errors[name] = df['_error'].iloc[0]
                    results[name] = pd.DataFrame(columns=['Time', 'Val'])
                else:
                    results[name] = df
            except Exception as e:
                n = futs[fut][0]; errors[n] = str(e)
                results[n] = pd.DataFrame(columns=['Time', 'Val'])
    return results, errors


# ============================================================
# PROCESS LOGIC (unchanged 100%)
# ============================================================
def process_logic(temp_df, conc_df, target_t, min_m):
    history = []
    TRIGGER_TEMP, MIN_DURATION, GAP_MIN = 40.0, 5.0, 45
    if temp_df.empty: return []

    combined_df = (pd.merge_asof(temp_df.sort_values('Time'),
                                 conc_df.sort_values('Time').rename(columns={'Val': 'Conc'}),
                                 on='Time', direction='backward')
                   if not conc_df.empty else temp_df.assign(Conc=0))

    raw_p, active, s_t = [], False, None
    for _, row in combined_df.iterrows():
        if row['Val'] > TRIGGER_TEMP and not active: active, s_t = True, row['Time']
        elif row['Val'] <= TRIGGER_TEMP and active:
            raw_p.append({'Start': s_t, 'End': row['Time']}); active = False
    if not raw_p: return []

    merged, curr = [], raw_p[0]
    for nxt in raw_p[1:]:
        if (nxt['Start'] - curr['End']).total_seconds() / 60 <= GAP_MIN: curr['End'] = nxt['End']
        else: merged.append(curr); curr = nxt
    merged.append(curr)

    for no, p in enumerate(merged, 1):
        mask = (combined_df['Time'] >= p['Start']) & (combined_df['Time'] <= p['End'])
        cyc  = combined_df.loc[mask].copy()
        if len(cyc) < 2: continue
        cyc  = cyc.set_index('Time').sort_index()
        cyc  = cyc[~cyc.index.duplicated(keep='first')]
        idx  = pd.date_range(start=p['Start'], end=p['End'], freq='10s')
        rs   = cyc.reindex(cyc.index.union(idx)).interpolate('linear').reindex(idx)
        acc  = (rs['Val'] >= target_t).sum() * (10 / 60)
        if (p['End'] - p['Start']).total_seconds() / 60 < MIN_DURATION: continue
        above = rs[rs['Val'] >= target_t]
        history.append({
            "No": no, "Start": p['Start'], "End": p['End'],
            "StartTime": p['Start'].strftime("%Y-%m-%d %H:%M"),
            "TotalDuration": int(round((p['End'] - p['Start']).total_seconds() / 60)),
            "TimeAboveTarget": int(round(acc)),
            "MaxTemp": int(round(cyc['Val'].max())),
            "AvgTemp": int(round(cyc['Val'].mean())),
            "AvgTempTarget": int(round(above['Val'].mean() if not above.empty else 0)),
            "AvgConc": round(cyc['Conc'].mean() if not cyc['Conc'].isna().all() else 0, 2),
            "Status": "PASS" if acc >= min_m else "FAIL"
        })
    return history


# ============================================================
# UI
# ============================================================
st.title("🛡️ CIP Performance Monitoring & Analytics")

with st.expander("📂 SYSTEM ACCESS & SETTINGS", expanded=True):
    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        user = st.text_input("Username", key="user")
        pw   = st.text_input("Password", type="password", key="pw")
    with c2:
        factory_choice = st.selectbox("Select Factory",
            options=list(FACTORY_CONFIG.keys()) + ["Summary All Plant"], index=3)
        target_t = st.number_input("Target Temp (°C)", value=70.0)
    with c3:
        min_m = st.number_input("Target Duration (Min)", value=40.0)
        s_dt  = st.date_input("Start Date", value=datetime(2026, 1, 1))
        e_dt  = st.date_input("End Date",   value=datetime.today())

    b1, b2 = st.columns([3, 1])
    with b1: execute_btn     = st.button("🚀 EXECUTE ANALYTICS", use_container_width=True)
    with b2: clear_cache_btn = st.button("🗑️ Clear Cache",       use_container_width=True)

if clear_cache_btn:
    n = sum(1 for f in os.listdir(CACHE_DIR)
            if f.endswith(".pkl") and not os.remove(os.path.join(CACHE_DIR, f)))
    st.success(f"✅ Cache cleared: {n} file(s)")


# ============================================================
# EXECUTE
# ============================================================
if execute_btn:
    if not user or not pw:
        st.error("Please enter Username and Password")
    else:
        auth = HTTPBasicAuth(user, pw)   # ← same as original, no domain needed
        st.session_state.results      = {}
        st.session_state.view_history = None
        st.session_state.fetch_errors = []

        with st.status("📊 Processing data...", expanded=True) as sb:

            if factory_choice != "Summary All Plant":
                f_conf = FACTORY_CONFIG[factory_choice]

                sb.write(f"🧪 Fetching chemical data for {factory_choice}...")
                df_conc = (get_data_pi(f_conf["cip_tag"], auth, s_dt, e_dt)
                           if f_conf["cip_tag"] else pd.DataFrame(columns=['Time', 'Val']))

                sb.write(f"🌡️ Fetching {len(f_conf['tags'])} tanks in parallel...")
                all_dfs, errs = fetch_all_tags_parallel(f_conf["tags"], auth, s_dt, e_dt)
                if errs: st.session_state.fetch_errors = errs

                for name, df_temp in all_dfs.items():
                    sb.write(f"⚙️ Analysing tank: {name}...")
                    if not df_temp.empty:
                        hist = process_logic(df_temp, df_conc, target_t, min_m)
                        if hist:
                            passed = sum(1 for h in hist if h["Status"] == "PASS")
                            st.session_state.results[name] = {
                                "summary": hist[-1],
                                "p_rate": round(passed / len(hist) * 100, 1),
                                "total": len(hist), "pass": passed, "list": hist,
                                "raw_temp": df_temp, "raw_conc": df_conc,
                                "factory": factory_choice
                            }

                found = len(st.session_state.results)
                sb.update(label=f"✅ Completed {factory_choice} — — data found {found}/{len(f_conf['tags'])} tank(s)",
                          state="complete", expanded=False)

            else:
                st.session_state.results = {"_is_summary": True}
                for f_name, f_conf in FACTORY_CONFIG.items():
                    sb.write(f"🏭 Fetching factory: {f_name}...")
                    st.session_state.results[f_name] = []
                    df_conc = (get_data_pi(f_conf["cip_tag"], auth, s_dt, e_dt)
                               if f_conf["cip_tag"] else pd.DataFrame(columns=['Time', 'Val']))
                    all_dfs, _ = fetch_all_tags_parallel(f_conf["tags"], auth, s_dt, e_dt)
                    for name, df_temp in all_dfs.items():
                        if not df_temp.empty:
                            for h in process_logic(df_temp, df_conc, target_t, min_m):
                                st.session_state.results[f_name].append({**h, "Tank": name})
                sb.update(label="✅ CompletedAll factory data loaded",
                          state="complete", expanded=False)

if st.session_state.fetch_errors:
    with st.expander(f"⚠️ Failed to fetch data for {len(st.session_state.fetch_errors)} tank(s)tank(s) failed to load"):
        for tank, err in st.session_state.fetch_errors.items():
            st.markdown(f'<div class="error-box">❌ <b>{tank}</b>: {err}</div>',
                        unsafe_allow_html=True)


# ============================================================
# DASHBOARD (unchanged 100%)
# ============================================================
if st.session_state.results:
    if "_is_summary" not in st.session_state.results:
        st.divider()
        st.subheader(f"🏭 Plant: {factory_choice}")
        cols = st.columns(4)
        # Sort tanks in order defined by FACTORY_CONFIG
        tag_order = FACTORY_CONFIG.get(factory_choice, {}).get("tags", {}).keys()
        ordered_tanks = [n for n in tag_order if n in st.session_state.results]
        if not ordered_tanks:  # fallback fallback for Summary All Plant
            ordered_tanks = list(st.session_state.results.keys())
        for i, name in enumerate(ordered_tanks):
            data = st.session_state.results[name]
            res = data["summary"]
            with cols[i % 4]:
                fig_g = go.Figure(go.Indicator(
                    mode="gauge", value=data['p_rate'],
                    gauge={'axis': {'range': [0,100], 'visible': False},
                           'bar': {'color': "#28a745" if data['p_rate'] >= 80 else "#dc3545"},
                           'bgcolor': "#ececec", 'borderwidth': 0}))
                fig_g.add_annotation(x=0.5, y=0.01, text=f"<b>{data['p_rate']}%</b>",
                                     showarrow=False, font=dict(size=18))
                fig_g.update_layout(height=100, margin=dict(l=10,r=10,t=10,b=10),
                                    paper_bgcolor='rgba(0,0,0,0)')
                st.markdown(
                    f"""<div class="tank-card {'status-pass' if res['Status']=='PASS' else 'status-fail'}">
                        <h4 style="margin:0;font-size:1.05em;color:#2c3e50;">{name}</h4>
                        <div class="latest-time">🕒 Latest: {res['StartTime']}</div>""",
                    unsafe_allow_html=True)
                st.plotly_chart(fig_g, use_container_width=True,
                                config={'displayModeBar': False, 'scrollZoom': True}, key=f"gauge_{name}")
                cip_d = f"{res['AvgConc']}%" if FACTORY_CONFIG[data["factory"]]["cip_tag"] else "N/A"
                st.markdown(
                    f"""<div style="font-size:0.68em;color:#7f8c8d;margin-top:-12px;margin-bottom:5px;">PASS {data['pass']}/{data['total']}</div>
                        <div class="metric-box">
                            ⏱️ <b>Time:</b> {res['TotalDuration']} min (<b>>{target_t}°C:</b> {res['TimeAboveTarget']} min)<br>
                            🌡️ <b>Temp avg:</b> {res['AvgTemp']}°C (<b>>{target_t}°C:</b> {res['AvgTempTarget']}°C)<br>
                            🔥 <b>Temp Max:</b> {res['MaxTemp']}°C<br>
                            🧪 <b>%CIP:</b> {cip_d}
                        </div></div>""", unsafe_allow_html=True)
                if st.button(f"🔍 HISTORY: {name}", key=f"btn_{name}", use_container_width=True):
                    st.session_state.view_history = name

        # --- Summary badge Overall stats for all tanks in factory ---
        all_results = [d for n, d in st.session_state.results.items() if n != "_is_summary"]
        if all_results:
            tc_all = sum(d["total"] for d in all_results)
            pc_all = sum(d["pass"]  for d in all_results)
            fc_all = tc_all - pc_all
            rt_all = round(pc_all / tc_all * 100, 1) if tc_all else 0
            st.markdown(
                f"""<div style="background:#fff;padding:12px;border-radius:10px;border:1px solid #eee;margin-bottom:10px;">
                    <span style="font-size:0.9em;font-weight:bold;color:#555;">📊 {factory_choice} Overall:</span>
                    <span class="summary-badge" style="background:#1a73e8;">Total: {tc_all}</span>
                    <span class="summary-badge" style="background:#28a745;">Pass: {pc_all}</span>
                    <span class="summary-badge" style="background:#dc3545;">Fail: {fc_all}</span>
                    <span class="summary-badge" style="background:#f39c12;">Success: {rt_all}%</span>
                </div>""", unsafe_allow_html=True)

        st.divider()
        st.subheader("📅 CIP Timeline")
        all_data = [dict(c, Tank=n) for n, d in st.session_state.results.items()
                    if n != "_is_summary" for c in d["list"]]
        if all_data:
            df_all = pd.DataFrame(all_data).sort_values("Start")
            fig_tl = go.Figure()
            for status, color in [("PASS","#28a745"),("FAIL","#dc3545")]:
                ds = df_all[df_all["Status"]==status]
                if not ds.empty:
                    fig_tl.add_trace(go.Bar(x=ds["Start"], y=[1]*len(ds),
                        name=status, marker_color=color,
                        customdata=ds[["Tank","TotalDuration","TimeAboveTarget","AvgConc","StartTime","End"]],
                        hovertemplate="<b>Tank: %{customdata[0]}</b><br>🕒 Start: %{customdata[4]}<br>⏱️ Duration: %{customdata[1]}m<br>🌡️ Time > Target: %{customdata[2]}m<br>🧪 %CIP: %{customdata[3]}%<extra></extra>"))
            fig_tl.update_layout(height=300, dragmode='pan',
                barmode='overlay',
                xaxis=dict(type='date', rangeslider=dict(visible=True)),
                yaxis=dict(visible=False, range=[0, 1.5]))
            st.plotly_chart(fig_tl, use_container_width=True, config={'scrollZoom': True})

    else:
        st.divider()
        st.subheader("🌍 Summary All Plant")

        # --- Monthly %Pass chart — all factories in one graph ---
        all_factory_data = []
        for f_name in FACTORY_CONFIG:
            for h in st.session_state.results.get(f_name, []):
                all_factory_data.append({**h, "Factory": f_name})

        if all_factory_data:
            df_all_f = pd.DataFrame(all_factory_data)
            df_all_f["Month"] = pd.to_datetime(df_all_f["Start"]).dt.to_period("M").astype(str)
            monthly = (df_all_f.groupby(["Factory","Month"])
                       .apply(lambda g: round(len(g[g["Status"]=="PASS"])/len(g)*100, 1))
                       .reset_index(name="%Pass"))
            monthly = monthly.sort_values("Month")

            FACTORY_COLORS = {"PK1":"#1a73e8","PK2":"#e8711a",
                               "KN":"#28a745","DC":"#9b27af","MCE":"#e8291a"}

            st.subheader("📈 Monthly %Pass Rate — All Factories")
            fig_m = go.Figure()
            for fn in FACTORY_CONFIG:
                dm = monthly[monthly["Factory"]==fn]
                if dm.empty: continue
                fig_m.add_trace(go.Bar(
                    x=dm["Month"], y=dm["%Pass"], name=fn,
                    marker_color=FACTORY_COLORS.get(fn,"#888"),
                    text=dm["%Pass"].astype(str)+"%",
                    textposition="outside",
                    textfont=dict(size=10),
                    hovertemplate=f"<b>{fn}</b><br>Month: %{{x}}<br>%Pass: %{{y}}%<extra></extra>"
                ))
            fig_m.add_hline(y=80, line_dash="dash", line_color="#dc3545", line_width=1.5,
                            annotation_text="Target 80%", annotation_position="bottom right",
                            annotation_font_color="#dc3545")
            fig_m.update_layout(
                barmode="group",
                dragmode='pan',
                height=450,
                yaxis=dict(title="%Pass Rate", range=[0,120], ticksuffix="%"),
                xaxis=dict(title="Month", tickangle=-30),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                plot_bgcolor="white", paper_bgcolor="white",
                margin=dict(t=60, b=60)
            )
            fig_m.update_xaxes(showgrid=False)
            fig_m.update_yaxes(showgrid=True, gridcolor="#f0f0f0")
            st.plotly_chart(fig_m, use_container_width=True, key="monthly_passrate", config={'scrollZoom': True})

        st.divider()
        for f_name in FACTORY_CONFIG:
            f_data = st.session_state.results.get(f_name, [])
            if not f_data: continue
            st.markdown(f"### 🏭 Factory: {f_name}")
            df_f = pd.DataFrame(f_data).sort_values("Start")
            tc, pc = len(df_f), len(df_f[df_f["Status"]=="PASS"])
            rate = round(pc/tc*100,1) if tc else 0
            fig_tl = go.Figure()
            for status, color in [("PASS","#28a745"),("FAIL","#dc3545")]:
                ds = df_f[df_f["Status"]==status]
                if not ds.empty:
                    fig_tl.add_trace(go.Bar(x=ds["Start"], y=[1]*len(ds),
                        name=status, marker_color=color,
                        customdata=ds[["Tank","TotalDuration","TimeAboveTarget","AvgConc","StartTime","End"]],
                        hovertemplate="<b>Tank: %{customdata[0]}</b><br>🕒 Start: %{customdata[4]}<br>⏱️ Duration: %{customdata[1]}m<br>🌡️ Time > Target: %{customdata[2]}m<br>🧪 %CIP: %{customdata[3]}%<extra></extra>"))
            fig_tl.update_layout(height=300, dragmode='pan',
                barmode='overlay',
                xaxis=dict(type='date', rangeslider=dict(visible=True)),
                yaxis=dict(visible=False, range=[0, 1.5]))
            st.plotly_chart(fig_tl, use_container_width=True, key=f"tl_{f_name}", config={'scrollZoom': True})
            st.markdown(
                f"""<div style="background:#fff;padding:12px;border-radius:10px;border:1px solid #eee;margin-top:-10px;margin-bottom:25px;">
                    <span style="font-size:0.9em;font-weight:bold;color:#555;">📊 {f_name} Stats:</span>
                    <span class="summary-badge" style="background:#1a73e8;">Total: {tc}</span>
                    <span class="summary-badge" style="background:#28a745;">Pass: {pc}</span>
                    <span class="summary-badge" style="background:#dc3545;">Fail: {tc-pc}</span>
                    <span class="summary-badge" style="background:#f39c12;">Success: {rate}%</span>
                </div>""", unsafe_allow_html=True)


# ============================================================
# HISTORY EXPLORER (unchanged 100%)
# ============================================================
if st.session_state.view_history and st.session_state.view_history in st.session_state.results:
    sel = st.session_state.view_history
    db  = st.session_state.results[sel]
    st.divider()
    st.subheader(f"📊 Detailed History: {sel} ({db['factory']})")
    hist_df  = pd.DataFrame(db["list"]).sort_values("StartTime", ascending=False)
    label_fn = lambda x: f"No. {x['No']} | {x['StartTime']} | {x['Status']}"
    opt      = st.selectbox("Select Item No.:", hist_df.apply(label_fn, axis=1).tolist())
    r_data   = hist_df[hist_df.apply(label_fn, axis=1) == opt].iloc[0]
    fig_h    = make_subplots(specs=[[{"secondary_y": True}]])
    mask     = ((db["raw_temp"]["Time"] >= r_data["Start"] - timedelta(minutes=10)) &
                (db["raw_temp"]["Time"] <= r_data["End"]   + timedelta(minutes=10)))
    fig_h.add_trace(go.Scatter(x=db["raw_temp"].loc[mask,'Time'],
                               y=db["raw_temp"].loc[mask,'Val'],
                               name="Temp (°C)", line=dict(color="#e74c3c", width=3)))
    if not db["raw_conc"].empty:
        cm = ((db["raw_conc"]["Time"] >= r_data["Start"] - timedelta(minutes=10)) &
              (db["raw_conc"]["Time"] <= r_data["End"]   + timedelta(minutes=10)))
        fig_h.add_trace(go.Scatter(x=db["raw_conc"].loc[cm,'Time'],
                                   y=db["raw_conc"].loc[cm,'Val'],
                                   name="%CIP Conc",
                                   line=dict(color="#3498db", width=2, dash='dot')),
                        secondary_y=True)
    fig_h.update_layout(xaxis_title="Time", yaxis_title="Temp (°C)", dragmode="pan")
    st.plotly_chart(fig_h, use_container_width=True, config={'scrollZoom': True})
    st.dataframe(hist_df.drop(columns=["Start","End"]), use_container_width=True, hide_index=True)
    if st.button("✖️ Close History"):
        st.session_state.view_history = None; st.rerun()