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

# --- 1. CONFIG & UI STYLE ---
st.set_page_config(page_title="CIP Monitoring & Analytics Pro", layout="wide")

# Custom CSS to hide the Streamlit "Deploy" button and hamburger menu
st.markdown("""
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .tank-card {
        border-radius: 15px; padding: 20px; text-align: center;
        background-color: #ffffff; box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        margin-bottom: 10px; border-top: 10px solid #ddd;
    }
    .status-pass { border-top-color: #28a745; }
    .status-fail { border-top-color: #dc3545; }
    .data-label { font-size: 0.85em; color: #7f8c8d; margin-top: 5px; }
    .data-value { font-size: 1.05em; font-weight: 700; color: #2c3e50; }
    .cip-badge {
        background-color: #eef2f7; color: #3498db;
        padding: 4px 15px; border-radius: 20px;
        font-weight: bold; font-size: 0.85em; display: inline-block;
        border: 1px solid #d6eaf8; margin-top: 8px;
    }
    .efficiency-text { font-size: 0.85em; font-weight: bold; color: #5d6d7e; margin-top: -10px; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. SESSION STATE ---
if "results" not in st.session_state: st.session_state.results = {}
if "view_history" not in st.session_state: st.session_state.view_history = None

TANK_MAP = {
    "Tank 421": "BEB3-10-0400-TT421", "Tank 422": "BEB3-10-0400-TT422B",
    "Tank 423": "BEB3-10-0400-TT423B", "Tank 424": "BEB3-10-0400-TT424B",
    "Tank 425": "BEB3-10-0400-TT425B", "Tank 426": "BEB3-10-0400-TT426B",
    "Tank 427": "BEB3-10-0400-TT427B"
}

# --- 3. DATA LOGIC ---
def get_data_pi(tag_path, auth, start_time):
    try:
        PI_BASE = "https://piazu.mitrphol.com/piwebapi"
        r = requests.get(f"{PI_BASE}/points", params={"path": f"\\\\MPAZU-PIDCDB\\{tag_path}"}, auth=auth, verify=False, timeout=15)
        
        if r.status_code == 401:
            st.error(f"❌ Login Failed (401): Invalid Credentials")
            return None
        if r.status_code != 200:
            st.error(f"⚠️ PI Error: Tag {tag_path} not found (Status: {r.status_code})")
            return pd.DataFrame(columns=['Time', 'Val'])
            
        webid = r.json()["WebId"]
        start_str = start_time.strftime("%Y-%m-%dT00:00:00Z")
        r_data = requests.get(f"{PI_BASE}/streams/{webid}/recorded", params={"startTime": start_str, "endTime": "*", "maxCount": 50000}, auth=auth, verify=False, timeout=25)
        
        items = r_data.json().get("Items", [])
        if not items: return pd.DataFrame(columns=['Time', 'Val'])
            
        df = pd.DataFrame(items)
        df['Time'] = pd.to_datetime(df['Timestamp'], format='ISO8601').dt.tz_localize(None)
        
        def clean_val(x):
            if isinstance(x, dict): return x.get('Value')
            return x
            
        df['Val'] = pd.to_numeric(df['Value'].apply(clean_val), errors='coerce')
        return df[['Time', 'Val']].dropna().sort_values('Time')
    except Exception as e:
        st.error(f"🚨 Connection Error: {str(e)}")
        return pd.DataFrame(columns=['Time', 'Val'])

def process_logic(temp_df, cip_df, target_t, min_m):
    history = []
    GAP_MIN = 60 
    raw_p, active, s_t = [], False, None

    if temp_df.empty: return []

    for _, row in temp_df.iterrows():
        if row['Val'] > target_t and not active:
            active, s_t = True, row['Time']
        elif row['Val'] <= target_t and active:
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
        dur = (p['End'] - p['Start']).total_seconds() / 60
        if dur >= 5:
            avg_t = round(temp_df.loc[(temp_df['Time'] >= p['Start']) & (temp_df['Time'] <= p['End']), 'Val'].mean(), 1)
            c_vals = cip_df.loc[(cip_df['Time'] >= p['Start']) & (cip_df['Time'] <= p['End']), 'Val']
            avg_c = round(c_vals.mean(), 2) if not c_vals.empty else (round(cip_df[cip_df['Time'] <= p['Start']].iloc[-1]['Val'], 2) if not cip_df[cip_df['Time'] <= p['Start']].empty else 0.0)
            is_pass = (dur >= min_m)
            
            history.append({
                "Start": p['Start'], "End": p['End'], 
                "StartTime": p['Start'].strftime("%Y-%m-%d %H:%M"), 
                "Duration": round(dur, 1), "AvgTemp": avg_t, 
                "AvgCIP": avg_c, "Status": "PASS" if is_pass else "FAIL"
            })
    return history

# --- 4. MAIN INTERFACE ---
st.title("🛡️ CIP Performance Monitoring & Analytics")

# Settings & Login Section on Main Page
with st.expander("🔑 SYSTEM ACCESS & THRESHOLD SETTINGS", expanded=True):
    set_col1, set_col2, set_col3 = st.columns([1, 1, 1])
    with set_col1:
        user = st.text_input("Username", placeholder="e.g. mp\yourname")
        pw = st.text_input("Password", type="password")
    with set_col2:
        t_tgt = st.number_input("Target Temp (°C)", value=65.0)
        m_tgt = st.number_input("Min Duration (Min)", value=40.0)
    with set_col3:
        # Default Start Date: 2026-01-01
        s_dt = st.date_input("Start Date", value=datetime(2026, 1, 1))
        st.write("") # Spacer
        execute_btn = st.button("🚀 EXECUTE ANALYTICS", use_container_width=True)

if execute_btn:
    if not user or not pw:
        st.warning("⚠️ Please enter Username and Password.")
    else:
        auth = HTTPBasicAuth(user, pw)
        st.session_state.results = {}
        
        with st.spinner("🔄 Fetching data from PI System..."):
            df_cip_all = get_data_pi("BEB3-57-0100-CIP", auth, s_dt)
            if df_cip_all is not None:
                found_any = False
                for name, tag in TANK_MAP.items():
                    df_temp = get_data_pi(tag, auth, s_dt)
                    if df_temp is not None and not df_temp.empty:
                        hist = process_logic(df_temp, df_cip_all, t_tgt, m_tgt)
                        if hist:
                            found_any, total_runs = True, len(hist)
                            pass_runs = sum(1 for h in hist if "PASS" in h["Status"])
                            st.session_state.results[name] = {
                                "summary": hist[-1], 
                                "p_rate": round((pass_runs / total_runs) * 100, 1), 
                                "total": total_runs, "pass": pass_runs,
                                "history_df": pd.DataFrame(hist), "raw_temp": df_temp, "raw_cip": df_cip_all
                            }
                if not found_any:
                    st.info("ℹ️ No cleaning cycles found for the selected period.")

# --- 5. DASHBOARD ---
if st.session_state.results:
    st.divider()
    cols = st.columns(4)
    for i, (name, data) in enumerate(st.session_state.results.items()):
        res = data["summary"]
        last_color = "#28a745" if "PASS" in res["Status"] else "#dc3545"
        rate_color = "#2ecc71" if data["p_rate"] >= 90 else ("#f1c40f" if data["p_rate"] >= 70 else "#e74c3c")
        
        with cols[i % 4]:
            st.markdown(f"""
                <div class="tank-card {'status-pass' if "PASS" in res["Status"] else 'status-fail'}">
                    <h3 style="margin-bottom:10px;">{name}</h3>
                    <div style="margin-bottom:15px;">
                        <svg width="45" height="65" viewBox="0 0 60 100">
                            <path d="M10 20 Q10 10 30 10 Q50 10 50 20 L50 80 Q50 90 30 90 Q10 90 10 80 Z" fill="{last_color}" stroke="#333" stroke-width="2"/>
                        </svg>
                    </div>
                    <div class="data-label">LAST WASH: {res['StartTime']}</div>
                    <div class="data-value">⏱️ {res['Duration']} min | 🌡️ {res['AvgTemp']}°C</div>
                    <div class="cip-badge">CONC: {res['AvgCIP']}%</div>
                </div>
            """, unsafe_allow_html=True)
            
            fig = go.Figure(go.Indicator(
                mode="gauge+number", value=data["p_rate"],
                number={'suffix': "%", 'font': {'size': 20, 'color': rate_color}},
                gauge={'axis': {'range': [None, 100], 'visible': False}, 'bar': {'color': rate_color}}
            ))
            fig.update_layout(height=80, margin=dict(l=30, r=30, t=10, b=10), paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True, key=f"g_{name}", config={'displayModeBar': False})
            st.markdown(f'<div class="efficiency-text">Total: {data["total"]} | Pass: {data["pass"]}</div>', unsafe_allow_html=True)
            
            if st.button(f"🔍 HISTORY: {name}", key=f"btn_{name}", use_container_width=True):
                st.session_state.view_history = name

# --- 6. HISTORY EXPLORER ---
if st.session_state.view_history:
    sel = st.session_state.view_history
    db = st.session_state.results[sel]
    st.divider()
    st.subheader(f"📊 Detailed History: {sel}")
    
    hist_df = db["history_df"].sort_values("Start", ascending=False)
    opt = st.selectbox("Select Cycle Trend:", hist_df.apply(lambda x: f"{x['StartTime']} | {x['Duration']}m | {x['Status']}", axis=1).tolist())
    r_data = hist_df[hist_df.apply(lambda x: f"{x['StartTime']} | {x['Duration']}m | {x['Status']}", axis=1) == opt].iloc[0]
    
    fig_t = make_subplots(specs=[[{"secondary_y": True}]])
    m_t = (db["raw_temp"]["Time"] >= r_data["Start"] - timedelta(minutes=15)) & (db["raw_temp"]["Time"] <= r_data["End"] + timedelta(minutes=15))
    m_c = (db["raw_cip"]["Time"] >= r_data["Start"] - timedelta(minutes=15)) & (db["raw_cip"]["Time"] <= r_data["End"] + timedelta(minutes=15))
    
    fig_t.add_trace(go.Scatter(x=db["raw_temp"].loc[m_t, 'Time'], y=db["raw_temp"].loc[m_t, 'Val'], name="Temp (°C)", line=dict(color="#e74c3c", width=2.5)), secondary_y=False)
    fig_t.add_trace(go.Scatter(x=db["raw_cip"].loc[m_c, 'Time'], y=db["raw_cip"].loc[m_c, 'Val'], name="% CIP", line=dict(color="#3498db", dash='dot')), secondary_y=True)
    fig_t.update_layout(hovermode="x unified", title=f"Cleaning Trend: {r_data['StartTime']}")
    st.plotly_chart(fig_t, use_container_width=True)
    
    st.write("#### 📝 History Log")
    styled_df = hist_df.drop(columns=["Start", "End"]).style.applymap(lambda x: 'color: #28a745; font-weight: bold' if x == "PASS" else 'color: #dc3545; font-weight: bold', subset=['Status'])
    st.dataframe(styled_df, use_container_width=True)
    
    if st.button("✖️ Close History"):
        st.session_state.view_history = None
        st.rerun()