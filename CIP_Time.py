import streamlit as st
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 1. CONFIG & DATA STRUCTURE ---
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

if "results" not in st.session_state: st.session_state.results = {}
if "view_history" not in st.session_state: st.session_state.view_history = None

st.markdown("""
    <style>
    header {visibility: hidden;}
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display:none;}
    .tank-card {
        border-radius: 15px; padding: 15px; text-align: center;
        background-color: #ffffff; box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        margin-bottom: 10px; border-top: 8px solid #ddd;
        display: flex; flex-direction: column; align-items: center;
    }
    .status-pass { border-top-color: #28a745; }
    .status-fail { border-top-color: #dc3545; }
    .metric-box { text-align: left; font-size: 0.82em; background: #f8f9fa; padding: 10px; border-radius: 8px; margin-top: 10px; line-height: 1.6; width: 100%; }
    .latest-time { font-size: 0.85em; color: #1a73e8; font-weight: bold; margin-bottom: 0px; }
    .summary-badge { padding: 5px 12px; border-radius: 20px; font-weight: bold; color: white; display: inline-block; margin-right: 5px; font-size: 0.8em; }
    </style>
    """, unsafe_allow_html=True)

def get_data_pi(tag_path, auth, start_time):
    if not tag_path: return pd.DataFrame(columns=['Time', 'Val'])
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
        df['Time'] = pd.to_datetime(df['Timestamp'], format='ISO8601').dt.tz_convert('Asia/Bangkok').dt.tz_localize(None)
        df['Val'] = pd.to_numeric(df['Value'].apply(lambda x: x.get('Value') if isinstance(x, dict) else x), errors='coerce')
        return df[['Time', 'Val']].dropna().sort_values('Time')
    except: return pd.DataFrame(columns=['Time', 'Val'])

def process_logic(temp_df, conc_df, target_t, min_m):
    history = []
    TRIGGER_TEMP, MIN_DURATION, GAP_MIN = 40.0, 5.0, 45
    if temp_df.empty: return []

    if not conc_df.empty:
        combined_df = pd.merge_asof(temp_df.sort_values('Time'), 
                                    conc_df.sort_values('Time').rename(columns={'Val': 'Conc'}), 
                                    on='Time', direction='backward')
    else:
        combined_df = temp_df.copy(); combined_df['Conc'] = 0

    raw_p, active, s_t = [], False, None
    for _, row in combined_df.iterrows():
        if row['Val'] > TRIGGER_TEMP and not active:
            active, s_t = True, row['Time']
        elif row['Val'] <= TRIGGER_TEMP and active:
            raw_p.append({'Start': s_t, 'End': row['Time']})
            active = False
    if not raw_p: return []

    merged, curr = [], raw_p[0]
    for next_p in raw_p[1:]:
        if (next_p['Start'] - curr['End']).total_seconds() / 60 <= GAP_MIN:
            curr['End'] = next_p['End']
        else: merged.append(curr); curr = next_p
    merged.append(curr)
    
    display_no = 1
    for p in merged:
        mask = (combined_df['Time'] >= p['Start']) & (combined_df['Time'] <= p['End'])
        this_cycle = combined_df.loc[mask].copy()
        if len(this_cycle) < 2: continue

        this_cycle = this_cycle.set_index('Time')
        this_cycle = this_cycle[~this_cycle.index.duplicated(keep='first')]
        

        new_index = pd.date_range(start=p['Start'], end=p['End'], freq='10s')
        resampled = this_cycle.reindex(this_cycle.index.union(new_index)).interpolate(method='linear')
        resampled = resampled.reindex(new_index)

        acc_min = (resampled['Val'] >= target_t).sum() * (10/60) 

        if (p['End'] - p['Start']).total_seconds() / 60 < MIN_DURATION: continue

        history.append({
            "No": display_no, 
            "Start": p['Start'], 
            "End": p['End'],
            "StartTime": p['Start'].strftime("%Y-%m-%d %H:%M"),
            "TotalDuration": int(round((p['End'] - p['Start']).total_seconds() / 60)),
            "TimeAboveTarget": int(round(acc_min)), 
            "MaxTemp": int(round(this_cycle['Val'].max())),
            "AvgTemp": int(round(this_cycle['Val'].mean())),
            "AvgTempTarget": int(round(resampled[resampled['Val'] >= target_t]['Val'].mean() if not resampled[resampled['Val'] >= target_t].empty else 0)),
            "AvgConc": round(this_cycle['Conc'].mean() if not this_cycle['Conc'].isna().all() else 0, 2), # %Conc อาจต้องเก็บทศนิยมไว้บ้าง
            "Status": "PASS" if acc_min >= min_m else "FAIL"
        })
        display_no += 1
    return history

# --- 3. UI & EXECUTION ---
st.title("🛡️ CIP Performance Monitoring & Analytics")
with st.expander("📂 SYSTEM ACCESS & SETTINGS", expanded=True):
    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        user, pw = st.text_input("Username", value=""), st.text_input("Password", type="password")
    with c2:
        factory_choice = st.selectbox("Select Factory", options=list(FACTORY_CONFIG.keys()) + ["Summary All Plant"], index=3)
        target_t = st.number_input("Target Temp (°C)", value=70.0)
    with c3:
        min_m = st.number_input("Target Duration (Min)", value=40.0)
        s_dt = st.date_input("Start Date", value=datetime(2026, 1, 1))
    execute_btn = st.button("🚀 EXECUTE ANALYTICS", use_container_width=True)

if execute_btn:
    auth = HTTPBasicAuth(user, pw)
    st.session_state.results = {}
    st.session_state.view_history = None
    if factory_choice != "Summary All Plant":
        f_conf = FACTORY_CONFIG[factory_choice]
        with st.spinner(f"🔄 Fetching data for {factory_choice}..."):
            df_conc_all = get_data_pi(f_conf["cip_tag"], auth, s_dt) if f_conf["cip_tag"] else pd.DataFrame(columns=['Time', 'Val'])
            for name, tag in f_conf["tags"].items():
                df_temp = get_data_pi(tag, auth, s_dt)
                if not df_temp.empty:
                    hist = process_logic(df_temp, df_conc_all, target_t, min_m)
                    if hist:
                        passed = sum(1 for h in hist if h["Status"] == "PASS")
                        st.session_state.results[name] = {
                            "summary": hist[-1], "p_rate": round((passed/len(hist))*100, 1),
                            "total": len(hist), "pass": passed, "list": hist, 
                            "raw_temp": df_temp, "raw_conc": df_conc_all, "factory": factory_choice
                        }
    else:
        st.session_state.results = {"_is_summary": True}
        for f_name, f_conf in FACTORY_CONFIG.items():
            st.session_state.results[f_name] = []
            with st.spinner(f"🔄 Fetching all data: {f_name}..."):
                df_conc_all = get_data_pi(f_conf["cip_tag"], auth, s_dt) if f_conf["cip_tag"] else pd.DataFrame(columns=['Time', 'Val'])
                for name, tag in f_conf["tags"].items():
                    df_temp = get_data_pi(tag, auth, s_dt)
                    if not df_temp.empty:
                        hist = process_logic(df_temp, df_conc_all, target_t, min_m)
                        for h in hist:
                            h_copy = h.copy(); h_copy["Tank"] = name
                            st.session_state.results[f_name].append(h_copy)

# --- 4. DASHBOARD RENDER ---
if st.session_state.results:
    if "_is_summary" not in st.session_state.results:
        st.divider()
        # Display specific plant name at top
        st.subheader(f"🏭 Plant: {factory_choice}")
        cols = st.columns(4)
        for i, (name, data) in enumerate(st.session_state.results.items()):
            res = data["summary"]
            with cols[i % 4]:
                fig_gauge = go.Figure(go.Indicator(
                    mode = "gauge", value = data['p_rate'],
                    gauge = {'axis': {'range': [0, 100], 'visible': False}, 'bar': {'color': "#28a745" if data['p_rate'] >= 80 else "#dc3545"}, 'bgcolor': "#ececec", 'borderwidth': 0},
                    domain = {'x': [0, 1], 'y': [0, 1]} 
                ))
                fig_gauge.add_annotation(x=0.5, y=0.01, text=f"<b>{data['p_rate']}%</b>", showarrow=False, font=dict(size=18, color="#2c3e50"))
                fig_gauge.update_layout(height=100, margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor='rgba(0,0,0,0)')

                st.markdown(f"""
                    <div class="tank-card {'status-pass' if res['Status']=='PASS' else 'status-fail'}">
                        <h4 style="margin:0; font-size: 1.05em; color:#2c3e50;">{name}</h4>
                        <div class="latest-time">🕒 Latest: {res['StartTime']}</div>
                """, unsafe_allow_html=True)
                st.plotly_chart(fig_gauge, use_container_width=True, config={'displayModeBar': False}, key=f"gauge_{name}")
                
                cip_display = f"{res['AvgConc']}%" if FACTORY_CONFIG[data["factory"]]["cip_tag"] else "N/A"
                st.markdown(f"""
                        <div style="font-size:0.68em; color:#7f8c8d; margin-top:-12px; margin-bottom:5px;">PASS {data['pass']}/{data['total']} </div>
                        <div class="metric-box">
                            ⏱️ <b>Time:</b> {res['TotalDuration']} min (<b>>{target_t}°C:</b> {res['TimeAboveTarget']} min)<br>
                            🌡️ <b>Temp avg:</b> {res['AvgTemp']}°C (<b>>{target_t}°C:</b> {res['AvgTempTarget']}°C)<br>
                            🔥 <b>Temp Max:</b> {res['MaxTemp']}°C<br>
                            🧪 <b>%CIP:</b> {cip_display}
                        </div>
                    </div>
                """, unsafe_allow_html=True)
                if st.button(f"🔍 HISTORY: {name}", key=f"btn_{name}", use_container_width=True):
                    st.session_state.view_history = name

        st.divider()
        st.subheader("📅 CIP Timeline")
        all_data = [dict(c, Tank=n) for n, d in st.session_state.results.items() if n != "_is_summary" for c in d["list"]]
        if all_data:
            df_all = pd.DataFrame(all_data).sort_values("Start")
            fig_timeline = go.Figure()
            for status, color in [("PASS", "#28a745"), ("FAIL", "#dc3545")]:
                df_s = df_all[df_all["Status"] == status]
                if not df_s.empty:
                    fig_timeline.add_trace(go.Bar(x=df_s["Start"], y=df_s["TotalDuration"], name=status, marker_color=color, customdata=df_s[["Tank", "TimeAboveTarget", "AvgConc"]], hovertemplate="<b>Tank: %{customdata[0]}</b><br>Duration: %{y}m<br>Time > Target: %{customdata[1]}m<br>Avg %CIP: %{customdata[2]}%<extra></extra>"))
            first_date = df_all["Start"].min()
            fig_timeline.update_layout(height=450, dragmode='pan', xaxis=dict(type='date', range=[first_date, first_date + timedelta(hours=24)], rangeslider=dict(visible=True, thickness=0.05)), yaxis=dict(title="Min"), margin=dict(t=30, b=10, l=50, r=50))
            st.plotly_chart(fig_timeline, use_container_width=True, config={'scrollZoom': True})
    
    else:
        st.divider()
        st.subheader("🌍 Summary All Plant")
        for f_name in FACTORY_CONFIG.keys():
            f_data = st.session_state.results.get(f_name, [])
            if f_data:
                st.markdown(f"### 🏭 Factory: {f_name}")
                df_f = pd.DataFrame(f_data).sort_values("Start")
                total_c, pass_c = len(df_f), len(df_f[df_f["Status"] == "PASS"])
                rate = round((pass_c/total_c)*100, 1) if total_c > 0 else 0
                fig_timeline = go.Figure()
                for status, color in [("PASS", "#28a745"), ("FAIL", "#dc3545")]:
                    df_s = df_f[df_f["Status"] == status]
                    if not df_s.empty:
                        fig_timeline.add_trace(go.Bar(x=df_s["Start"], y=df_s["TotalDuration"], name=status, marker_color=color, customdata=df_s[["Tank", "TimeAboveTarget", "AvgConc"]], hovertemplate="<b>Tank: %{customdata[0]}</b><br>Duration: %{y}m<br>Time > Target: %{customdata[1]}m<br>Avg %CIP: %{customdata[2]}%<extra></extra>"))
                first_date = df_f["Start"].min()
                fig_timeline.update_layout(height=400, dragmode='pan', xaxis=dict(type='date', range=[first_date, first_date + timedelta(hours=24)], rangeslider=dict(visible=True, thickness=0.05)), yaxis=dict(fixedrange=True, title="Min"), margin=dict(t=30, b=10, l=50, r=50))
                st.plotly_chart(fig_timeline, use_container_width=True, key=f"timeline_{f_name}", config={'scrollZoom': True})
                st.markdown(f"""<div style="background:#fff; padding:12px; border-radius:10px; border:1px solid #eee; margin-top:-10px; margin-bottom:25px;"><span style="font-size:0.9em; font-weight:bold; color:#555;">📊 {f_name} Stats:</span> <span class="summary-badge" style="background:#1a73e8;">Total: {total_c}</span> <span class="summary-badge" style="background:#28a745;">Pass: {pass_c}</span> <span class="summary-badge" style="background:#dc3545;">Fail: {total_c-pass_c}</span> <span class="summary-badge" style="background:#f39c12;">Success: {rate}%</span></div>""", unsafe_allow_html=True)
                st.divider()

# --- 5. HISTORY EXPLORER ---
if st.session_state.view_history and st.session_state.view_history in st.session_state.results:
    sel = st.session_state.view_history
    db = st.session_state.results[sel]
    st.divider()
    st.subheader(f"📊 Detailed History: {sel} ({db['factory']})")
    hist_df = pd.DataFrame(db["list"]).sort_values("StartTime", ascending=False)
    opt = st.selectbox("Select Item No.:", hist_df.apply(lambda x: f"No. {x['No']} | {x['StartTime']} | {x['Status']}", axis=1).tolist())
    r_data = hist_df[hist_df.apply(lambda x: f"No. {x['No']} | {x['StartTime']} | {x['Status']}", axis=1) == opt].iloc[0]
    fig_hist = make_subplots(specs=[[{"secondary_y": True}]])
    mask = (db["raw_temp"]["Time"] >= r_data["Start"] - timedelta(minutes=10)) & (db["raw_temp"]["Time"] <= r_data["End"] + timedelta(minutes=10))
    fig_hist.add_trace(go.Scatter(x=db["raw_temp"].loc[mask, 'Time'], y=db["raw_temp"].loc[mask, 'Val'], name="Temp (°C)", line=dict(color="#e74c3c", width=3)))
    if not db["raw_conc"].empty:
        conc_mask = (db["raw_conc"]["Time"] >= r_data["Start"] - timedelta(minutes=10)) & (db["raw_conc"]["Time"] <= r_data["End"] + timedelta(minutes=10))
        fig_hist.add_trace(go.Scatter(x=db["raw_conc"].loc[conc_mask, 'Time'], y=db["raw_conc"].loc[conc_mask, 'Val'], name="%CIP Conc", line=dict(color="#3498db", width=2, dash='dot')), secondary_y=True)
    fig_hist.update_layout(xaxis_title="Time", yaxis_title="Temp (°C)")
    st.plotly_chart(fig_hist, use_container_width=True)
    st.dataframe(hist_df.drop(columns=["Start", "End"]), use_container_width=True)
    if st.button("✖️ Close History"):
        st.session_state.view_history = None
        st.rerun()