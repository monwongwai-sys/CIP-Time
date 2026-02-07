import streamlit as st
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import urllib3

# ปิดคำเตือน Insecure Request
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 1. CONFIG & UI STYLE ---
st.set_page_config(page_title="CIP Monitoring & Analytics System", layout="wide")
st.markdown("""
    <style>
    .tank-card {
        border-radius: 15px; padding: 20px; text-align: center;
        background-color: #ffffff; box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        margin-bottom: 20px; border-top: 10px solid #ddd;
        transition: transform 0.3s ease;
    }
    .tank-card:hover { transform: translateY(-5px); }
    .status-pass { border-top-color: #28a745; }
    .status-fail { border-top-color: #dc3545; }
    .cip-badge {
        background-color: #f8f9fa; color: #1976d2;
        padding: 5px 15px; border-radius: 20px;
        font-weight: bold; display: inline-block; margin-top: 10px;
        border: 1px solid #dee2e6;
    }
    </style>
    """, unsafe_allow_html=True)

# --- 2. SESSION STATE ---
if "results" not in st.session_state:
    st.session_state.results = {}
if "view_history" not in st.session_state:
    st.session_state.view_history = None

TANK_MAP = {
    "Tank 421": "BEB3-10-0400-TT421", "Tank 422B": "BEB3-10-0400-TT422B",
    "Tank 423B": "BEB3-10-0400-TT423B", "Tank 424B": "BEB3-10-0400-TT424B",
    "Tank 425B": "BEB3-10-0400-TT425B", "Tank 426B": "BEB3-10-0400-TT426B",
    "Tank 427B": "BEB3-10-0400-TT427B"
}

# --- 3. HELPER FUNCTIONS ---
def get_pass_rate_color(rate):
    if rate >= 90: return "#28a745" # เขียว
    if rate >= 70: return "#ffc107" # เหลือง
    return "#dc3545" # แดง

def get_data_pi(tag_path, auth, start_time):
    try:
        PI_BASE = "https://piazu.mitrphol.com/piwebapi"
        r = requests.get(f"{PI_BASE}/points", params={"path": f"\\\\MPAZU-PIDCDB\\{tag_path}"}, auth=auth, verify=False, timeout=15)
        r.raise_for_status()
        webid = r.json()["WebId"]
        start_str = start_time.strftime("%Y-%m-%dT00:00:00Z")
        r_data = requests.get(f"{PI_BASE}/streams/{webid}/recorded", params={"startTime": start_str, "endTime": "*", "maxCount": 50000}, auth=auth, verify=False, timeout=25)
        items = r_data.json().get("Items", [])
        if not items: return pd.DataFrame(columns=['Time', 'Val'])
        df = pd.DataFrame(items)
        df['Time'] = pd.to_datetime(df['Timestamp'], format='ISO8601').dt.tz_localize(None)
        df['Val'] = pd.to_numeric(df['Value'].apply(lambda x: x.get('Value') if isinstance(x, dict) else x), errors='coerce')
        return df[['Time', 'Val']].dropna().sort_values('Time')
    except:
        return pd.DataFrame(columns=['Time', 'Val'])

def process_logic(temp_df, cip_df, target_t, min_m, c_min, c_max):
    history = []
    GAP_MIN = 60 
    raw_p = []
    active, s_t = False, None
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
            mask = (cip_df['Time'] >= p['Start']) & (cip_df['Time'] <= p['End'])
            c_vals = cip_df.loc[mask, 'Val']
            avg_c = round(c_vals.mean(), 2) if not c_vals.empty else (round(cip_df[cip_df['Time'] <= p['Start']].iloc[-1]['Val'], 2) if not cip_df[cip_df['Time'] <= p['Start']].empty else 0.0)
            is_pass = (dur >= min_m) and (c_min <= avg_c <= c_max)
            history.append({"Start": p['Start'], "End": p['End'], "StartTime": p['Start'].strftime("%Y-%m-%d %H:%M"), "Duration": round(dur, 1), "AvgCIP": avg_c, "Status": "✅ ผ่าน" if is_pass else "❌ ไม่ผ่าน"})
    return history

# --- 4. SIDEBAR ---
with st.sidebar:
    st.header("🔐 Authentication")
    user = st.text_input("User (domain\\user)")
    pw = st.text_input("Password", type="password")
    st.divider()
    st.header("⚙️ Setting Targets")
    t_target = st.number_input("Target Temp (°C)", value=65.0)
    m_target = st.number_input("Target Duration (Min)", value=40.0)
    c_low, c_high = 5.0, 10.0
    s_date = st.date_input("Analysis Start Date", value=pd.to_datetime("2026-01-01"))
    cip_tag_path = "BEB3-57-0100-CIP"

# --- 5. MAIN PROCESSING ---
st.title("🧪 Tank CIP Analytics & History Explorer")

if st.button("🚀 Run Analysis All Tanks"):
    if not (user and pw): st.warning("Please enter credentials.")
    else:
        auth = HTTPBasicAuth(user, pw)
        with st.spinner("Processing PI Data..."):
            df_cip_all = get_data_pi(cip_tag_path, auth, s_date)
            for name, tag in TANK_MAP.items():
                df_temp = get_data_pi(tag, auth, s_date)
                hist = process_logic(df_temp, df_cip_all, t_target, m_target, c_low, c_high)
                if hist:
                    p_count = sum(1 for h in hist if "✅" in h["Status"])
                    st.session_state.results[name] = {
                        "summary": {
                            "last_wash": hist[-1]["StartTime"], "last_dur": hist[-1]["Duration"],
                            "last_cip": hist[-1]["AvgCIP"], "is_pass": "✅" in hist[-1]["Status"],
                            "pass_rate": round((p_count / len(hist)) * 100, 1)
                        },
                        "history_df": pd.DataFrame(hist), "raw_temp": df_temp, "raw_cip": df_cip_all
                    }
        st.success("Data Updated!")

# --- 6. DISPLAY DASHBOARD ---
if st.session_state.results:
    st.divider()
    cols = st.columns(4)
    for i, (name, data) in enumerate(st.session_state.results.items()):
        res = data["summary"]
        last_color = "#28a745" if res["is_pass"] else "#dc3545"
        rate_color = get_pass_rate_color(res["pass_rate"])
        
        with cols[i % 4]:
            st.markdown(f"""
                <div class="tank-card {'status-pass' if res['is_pass'] else 'status-fail'}">
                    <h3 style="margin-bottom:10px;">{name}</h3>
                    <svg width="50" height="80" viewBox="0 0 60 100">
                        <path d="M10 20 Q10 10 30 10 Q50 10 50 20 L50 80 Q50 90 30 90 Q10 90 10 80 Z" fill="{last_color}" stroke="#333" stroke-width="2"/>
                    </svg>
                    <p style="margin-top:10px; font-size:0.85em; color:#666;">Last: {res['last_wash']}</p>
                    <p style="margin:0;">⏱️ <b>{res['last_dur']} Min</b></p>
                    <div class="cip-badge">%CIP: {res['last_cip']}%</div>
                    <hr style="margin:12px 0; border:0.1px solid #eee;">
                    <p style="margin:0; font-size:0.8em; color:#999;">OVERALL EFFICIENCY</p>
                    <p style="margin:0; font-weight:bold; color:{rate_color}; font-size:1.5em;">{res['pass_rate']}%</p>
                </div>
            """, unsafe_allow_html=True)
            if st.button(f"🔍 History & Trend {name}", key=f"btn_{name}"):
                st.session_state.view_history = name

# --- 7. HISTORY EXPLORER & INTERACTIVE TREND ---
if st.session_state.view_history:
    sel = st.session_state.view_history
    db = st.session_state.results[sel]
    st.divider()
    
    st.subheader(f"📊 Deep Dive: {sel}")
    
    # 1. รอบการล้างย้อนหลัง (Dropdown)
    hist_df = db["history_df"].sort_values("Start", ascending=False)
    options = hist_df.apply(lambda x: f"{x['StartTime']} | {x['Duration']} min | {x['Status']}", axis=1).tolist()
    selected_opt = st.selectbox("🎯 เลือกรอบการล้างเพื่อดูกราฟอุณหภูมิ:", options)
    
    # ดึงข้อมูลรอบที่เลือก
    round_data = hist_df[hist_df.apply(lambda x: f"{x['StartTime']} | {x['Duration']} min | {x['Status']}", axis=1) == selected_opt].iloc[0]
    
    # 2. วาดกราฟ Trend
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    m_t = (db["raw_temp"]["Time"] >= round_data["Start"] - timedelta(minutes=20)) & (db["raw_temp"]["Time"] <= round_data["End"] + timedelta(minutes=20))
    m_c = (db["raw_cip"]["Time"] >= round_data["Start"] - timedelta(minutes=20)) & (db["raw_cip"]["Time"] <= round_data["End"] + timedelta(minutes=20))
    
    fig.add_trace(go.Scatter(x=db["raw_temp"].loc[m_t, 'Time'], y=db["raw_temp"].loc[m_t, 'Val'], name="Temp (°C)", line=dict(color="#dc3545", width=3)), secondary_y=False)
    fig.add_trace(go.Scatter(x=db["raw_cip"].loc[m_c, 'Time'], y=db["raw_cip"].loc[m_c, 'Val'], name="%CIP", line=dict(color="#007bff", dash='dot', width=2)), secondary_y=True)
    
    fig.update_layout(title=f"Trend Analysis: {selected_opt}", hovermode="x unified", height=450)
    st.plotly_chart(fig, use_container_width=True)
    
    # 3. ตารางและปุ่ม Download
    st.write("#### 📜 All History Records")
    st.dataframe(hist_df.drop(columns=["Start", "End"]), use_container_width=True)
    
    csv_data = hist_df.drop(columns=["Start", "End"]).to_csv(index=False).encode('utf-8-sig')
    st.download_button("📥 Download This Tank's History (CSV)", data=csv_data, file_name=f"CIP_{sel}.csv")
    
    if st.button("✖️ Close Explorer"):
        st.session_state.view_history = None
        st.rerun()