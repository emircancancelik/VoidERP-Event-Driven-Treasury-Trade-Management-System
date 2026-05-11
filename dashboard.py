import streamlit as st
import pandas as pd
import random
import time
from datetime import datetime

# --- Ayarlar ---
st.set_page_config(
    page_title="VoidERP | Autonomous Treasury",
    page_icon="🧿",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Sahte Veri Üreticisi (Jüri Simülasyonu İçin) ---
def generate_mock_log():
    tx_id = f"TXN-{random.randint(10000000, 99999999)}"
    amount = round(random.uniform(1000, 150000), 2)
    
    # %70 Onay, %30 Ret Senaryosu
    if random.random() > 0.3:
        status = "APPROVED"
        reason = "Kelly Criterion & ECE Passed"
        calibrated_trust = round(random.uniform(0.70, 0.95), 2)
    else:
        status = "DROP"
        reason = random.choice(["high_ece_error", "low_calibrated_trust", "duplicate_invoice"])
        calibrated_trust = round(random.uniform(0.10, 0.45), 2)

    return {
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "tx_id": tx_id,
        "amount_usd": amount,
        "status": status,
        "reason": reason,
        "trust_score": calibrated_trust
    }

# --- Streamlit Arayüzü ---
st.title("VoidERP: Autonomous Treasury Orchestrator")
st.markdown("### Real-time AI Decision & ECE Calibration Monitor")

# Metrik Kartları
col1, col2, col3, col4 = st.columns(4)
metric_total_tx = col1.empty()
metric_approved = col2.empty()
metric_dropped = col3.empty()
metric_avg_trust = col4.empty()

st.markdown("---")

# Canlı Log Tablosu
st.subheader("Live Execution Stream")
log_container = st.empty()

# --- State Management ---
if 'log_data' not in st.session_state:
    st.session_state.log_data = []

if 'stats' not in st.session_state:
    st.session_state.stats = {"total": 0, "approved": 0, "dropped": 0, "trust_sum": 0}

# --- Canlı Döngü ---
if st.button("Start Live Feed"):
    st.session_state.running = True

if st.button("Stop"):
    st.session_state.running = False

if st.session_state.get('running', False):
    while True:
        new_log = generate_mock_log()
        
        st.session_state.log_data.insert(0, new_log)
        st.session_state.log_data = st.session_state.log_data[:50]
        
        st.session_state.stats["total"] += 1
        if new_log["status"] == "APPROVED":
            st.session_state.stats["approved"] += 1
        else:
            st.session_state.stats["dropped"] += 1
        st.session_state.stats["trust_sum"] += new_log["trust_score"]

        df = pd.DataFrame(st.session_state.log_data)
        
        def color_status(val):
            color = 'green' if val == 'APPROVED' else 'red'
            return f'color: {color}; font-weight: bold'
        
        styled_df = df.style.map(color_status, subset=['status'])
        
        with log_container:
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
            
        avg_trust = st.session_state.stats["trust_sum"] / st.session_state.stats["total"] if st.session_state.stats["total"] > 0 else 0
        
        metric_total_tx.metric("Total Processed", st.session_state.stats["total"])
        metric_approved.metric("Approved (Risk Checked)", st.session_state.stats["approved"])
        metric_dropped.metric("Dropped (AI Hallucination)", st.session_state.stats["dropped"])
        metric_avg_trust.metric("Avg Calibrated Trust", f"%{int(avg_trust*100)}")

        time.sleep(1.5)