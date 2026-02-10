import streamlit as st
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import urllib3

# ปิดคำเตือน SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 1. CONFIG & UI STYLE ---
st.set_page_config(page_title="CIP Monitoring & Analytics Pro", layout="wide")

if "results" not in st.session_state: st.session_state.results = {}
if "view_history" not in st.session_state: st.session_state.view_history = None

st.markdown("""
    <style>
    /* ซ่อน Toolbar มุมขวาบน (GitHub, Share) */
    header {visibility: hidden;}
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display:none;}

    .tank-card {
        border-radius: 15px; padding: 15px; text-align: center;
        background-color: #ffffff; box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        margin-bottom: 10px; border-top: 8px solid #ddd;
    }
    .status-pass { border-top-color: #28a745; }
    .status-fail { border-top-color: #dc3545; }
    .metric-box { text-align: left; font-size: 0.82em; background: #f8f9fa; padding: 10px; border-radius: 8px; margin-top: 10px; line-height: 1.6; }
    .latest-time { font-size: 0.85em; color: #1a73e8; font-weight: bold; margin-bottom: 5px; }
    </style>
    """, unsafe_allow_html=True)

TANK_MAP = {
    "R421": "BEB3-10-0400-TT421", "R422": "BEB3-10-0400-TT422B",
    "R423": "BEB3-10-0400-TT423B", "R424": "BEB3-10-0400-TT424B",
    "R425": "BEB3-10-0400-TT425B", "R426": "BEB3-10-0400-TT426B",
    "R427": "BEB3-10-0400-TT427B"
}
CIP_CONC_TAG = "BEB3-57-0100-CIP"

# --- 2. DATA LOGIC ---
def get_data_pi(tag_path, auth, start_time):
    try:
        PI_BASE = "https://piazu.mitrphol.com/piwebapi"
        r = requests.get(f"{PI_BASE}/points", params={"path": f"\\\\MPAZU-PIDCDB\\{tag_path}"}, auth=auth, verify=False, timeout=15)
        if r.status_code != 200: return pd.DataFrame(columns=['Time', 'Val'])
        webid = r.json()["WebId"]
        start_str = start_time.strftime("%Y-%m-%dT00:00:00Z")
        r_data = requests.get(f"{PI_BASE}/streams/{webid}/recorded", params={"startTime": start_str, "endTime": "*", "maxCount": 50000}, auth=auth, verify=False, timeout=25)
        items = r_data.json().get("Items", [])
        if not items: return pd.DataFrame(columns=['Time', 'Val'])
        df = pd.DataFrame(items)
        df['Time'] = pd.to_datetime(df['Timestamp'], format='ISO8601').dt.tz_localize(None)
        df['Val'] = pd.to_numeric(df['Value'].apply(lambda x: x.get('Value') if isinstance(x, dict) else x), errors='coerce')
        return df[['Time', 'Val']].dropna().sort_values('Time')
    except: return pd.DataFrame(columns=['Time', 'Val'])

def process_logic(temp_df, conc_df, target_t, min_m):
    history = []
    TRIGGER_TEMP, MIN_DURATION, GAP_MIN = 40.0, 5.0, 45
    
    if temp_df.empty: return []

    if not conc_df.empty:
        combined_df = pd.merge_asof(
            temp_df.sort_values('Time'), 
            conc_df.sort_values('Time').rename(columns={'Val': 'Conc'}), 
            on='Time', 
            direction='backward'
        )
    else:
        combined_df = temp_df.copy()
        combined_df['Conc'] = 0

    raw_p, active, s_t = [], False, None
    for _, row in combined_df.iterrows():
        if row['Val'] > TRIGGER_TEMP and not active:
            active, s_t = True, row['Time']
        elif row['Val'] <= TRIGGER_TEMP and active:
            raw_p.append({'Start': s_t, 'End': row['Time']})
            active = False
    if not raw_p: return []
    
    merged = []
    curr = raw_p[0]
    for next_p in raw_p[1:]:
        if (next_p['Start'] - curr['End']).total_seconds() / 60 <= GAP_MIN:
            curr['End'] = next_p['End']
        else:
            merged.append(curr); curr = next_p
    merged.append(curr)
    
    for p in merged:
        this_cycle = combined_df.loc[(combined_df['Time'] >= p['Start']) & (combined_df['Time'] <= p['End'])].copy()
        if this_cycle.empty: continue
        this_cycle['diff'] = this_cycle['Time'].diff().dt.total_seconds() / 60
        above_target = this_cycle[this_cycle['Val'] >= target_t]
        
        acc_min = above_target['diff'].sum()
        avg_overall = this_cycle['Val'].mean()
        avg_target = above_target['Val'].mean() if not above_target.empty else 0
        avg_conc = this_cycle['Conc'].mean() if not this_cycle['Conc'].isna().all() else 0
        
        total_dur = (p['End'] - p['Start']).total_seconds() / 60
        if total_dur < MIN_DURATION: continue
        
        history.append({
            "Start": p['Start'], "End": p['End'],
            "StartTime": p['Start'].strftime("%Y-%m-%d %H:%M"),
            "TotalDuration": round(total_dur, 1),
            "TimeAboveTarget": round(acc_min, 1),
            "MaxTemp": round(this_cycle['Val'].max(), 1),
            "AvgTemp": round(avg_overall, 1),
            "AvgTempTarget": round(avg_target, 1),
            "AvgConc": round(avg_conc, 2),
            "Status": "PASS" if acc_min >= min_m else "FAIL"
        })
    return history

# --- 3. UI & EXECUTION ---
st.title("🛡️ CIP Performance Monitoring & Analytics")
with st.expander("📂 SYSTEM ACCESS & SETTINGS", expanded=True):
    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        user = st.text_input("Username", value="")
        pw = st.text_input("Password", type="password")
    with c2:
        target_t = st.number_input("Target Temp (°C)", value=70.0)
        min_m = st.number_input("Target Duration (Min)", value=40.0)
    with c3:
        s_dt = st.date_input("Start Date", value=datetime(2025, 12, 31))
        execute_btn = st.button("🚀 EXECUTE ANALYTICS", use_container_width=True)

if execute_btn:
    auth = HTTPBasicAuth(user, pw)
    st.session_state.results = {}
    with st.spinner("🔄 Fetch data from the PI API..."):
        df_conc_all = get_data_pi(CIP_CONC_TAG, auth, s_dt)
        for name, tag in TANK_MAP.items():
            df_temp = get_data_pi(tag, auth, s_dt)
            if not df_temp.empty:
                hist = process_logic(df_temp, df_conc_all, target_t, min_m)
                if hist:
                    passed = sum(1 for h in hist if h["Status"] == "PASS")
                    st.session_state.results[name] = {
                        "summary": hist[-1], "p_rate": round((passed/len(hist))*100, 1),
                        "total": len(hist), "pass": passed, "list": hist, 
                        "raw_temp": df_temp, "raw_conc": df_conc_all
                    }

# --- 4. DASHBOARD RENDER ---
if st.session_state.results:
    st.divider()
    cols = st.columns(4)
    for i, (name, data) in enumerate(st.session_state.results.items()):
        res = data["summary"]
        with cols[i % 4]:
            fig_gauge = go.Figure(go.Indicator(
                mode = "gauge+number", 
                value = data['p_rate'],
                number = {'suffix': "%", 'font': {'size': 22, 'color': "#2c3e50"}},
                gauge = {
                    'axis': {'range': [0, 100], 'visible': False},
                    'bar': {'color': "#28a745" if data['p_rate'] >= 80 else "#dc3545"},
                    'bgcolor': "#f1f1f1", 
                    'borderwidth': 0
                }
            ))
            fig_gauge.update_layout(height=110, margin=dict(l=15, r=15, t=10, b=10))

            st.markdown(f"""
                <div class="tank-card {'status-pass' if res['Status']=='PASS' else 'status-fail'}">
                    <h4 style="margin:0;">{name}</h4>
                    <div class="latest-time">🕒 Latest: {res['StartTime']}</div>
            """, unsafe_allow_html=True)
            st.plotly_chart(fig_gauge, use_container_width=True, config={'displayModeBar': False}, key=f"gauge_{name}")
            st.markdown(f"""
                    <div style="font-size:0.75em; color:#7f8c8d; margin-top:-10px;">PASS {data['pass']}/{data['total']} </div>
                    <div class="metric-box">
                        ⏱️ <b>Time:</b> {res['TotalDuration']} min (<b>>{target_t} °C:</b> {res['TimeAboveTarget']} min)<br>
                        🌡️ <b>Temp avg:</b> {res['AvgTemp']} °C (<b>>{target_t} °C:</b> {res['AvgTempTarget']} °C)<br>
                        🧪 <b>%CIP:</b> {res['AvgConc']}%<br>
                        🔥 <b>Max Temp:</b> {res['MaxTemp']}°C
                    </div>
                </div>
            """, unsafe_allow_html=True)
            if st.button(f"🔍 HISTORY: {name}", key=f"btn_{name}", use_container_width=True):
                st.session_state.view_history = name

    # --- 4.5 GLOBAL TIMELINE ---
    st.divider()
    st.subheader("📅 CIP Timeline")
    all_data = []
    for n, d in st.session_state.results.items():
        for c in d["list"]:
            c_copy = c.copy(); c_copy["Tank"] = n
            all_data.append(c_copy)
    if all_data:
        df_all = pd.DataFrame(all_data).sort_values("Start")
        fig_timeline = go.Figure()
        for status, color in [("PASS", "#28a745"), ("FAIL", "#dc3545")]:
            df_s = df_all[df_all["Status"] == status]
            if not df_s.empty:
                fig_timeline.add_trace(go.Bar(
                    x=df_s["Start"], y=df_s["TotalDuration"], name=status, marker_color=color,
                    customdata=df_s[["Tank", "TimeAboveTarget", "AvgConc"]],
                    hovertemplate="<b>Tank: %{customdata[0]}</b><br>Duration: %{y}m<br>Time > Target: %{customdata[1]}m<br>Avg %CIP: %{customdata[2]}%<extra></extra>"
                ))
        first_date = df_all["Start"].min()
        initial_view_end = first_date + timedelta(hours=24)
        fig_timeline.update_layout(height=450, dragmode='pan', xaxis=dict(type='date', tickformat="%d %b %H:%M", range=[first_date, initial_view_end], rangeslider=dict(visible=True, thickness=0.05)), yaxis=dict(fixedrange=True), hovermode="closest", margin=dict(t=30, b=10, l=50, r=50))
        st.plotly_chart(fig_timeline, use_container_width=True, key="main_timeline_chart", config={'scrollZoom': True})

# --- 5. HISTORY EXPLORER ---
if st.session_state.view_history:
    sel = st.session_state.view_history
    db = st.session_state.results[sel]
    st.divider()
    st.subheader(f"📊 Detailed History: {sel}")
    hist_df = pd.DataFrame(db["list"]).sort_values("StartTime", ascending=False)
    opt = st.selectbox("Select Cycle:", hist_df.apply(lambda x: f"{x['StartTime']} | {x['Status']}", axis=1).tolist())
    r_data = hist_df[hist_df.apply(lambda x: f"{x['StartTime']} | {x['Status']}", axis=1) == opt].iloc[0]
    
    fig_hist = make_subplots(specs=[[{"secondary_y": True}]])
    mask = (db["raw_temp"]["Time"] >= r_data["Start"] - timedelta(minutes=10)) & (db["raw_temp"]["Time"] <= r_data["End"] + timedelta(minutes=10))
    conc_mask = (db["raw_conc"]["Time"] >= r_data["Start"] - timedelta(minutes=10)) & (db["raw_conc"]["Time"] <= r_data["End"] + timedelta(minutes=10))
    
    fig_hist.add_trace(go.Scatter(x=db["raw_temp"].loc[mask, 'Time'], y=db["raw_temp"].loc[mask, 'Val'], name="Temp (°C)", line=dict(color="#e74c3c", width=3)))
    fig_hist.add_trace(go.Scatter(x=db["raw_conc"].loc[conc_mask, 'Time'], y=db["raw_conc"].loc[conc_mask, 'Val'], name="%CIP Conc", line=dict(color="#3498db", width=2, dash='dot')), secondary_y=True)
    fig_hist.add_hline(y=target_t, line_dash="dash", line_color="green", annotation_text=f"Target {target_t}°C")
    
    st.plotly_chart(fig_hist, use_container_width=True, key="history_detail_chart")
    st.dataframe(hist_df.drop(columns=["Start", "End"]), use_container_width=True)
    if st.button("✖️ Close History", key="close_history_btn"):
        st.session_state.view_history = None
        st.rerun()