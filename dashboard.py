import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import time
import random
from datetime import datetime

# --- PROFESYONEL TEMA AYARLARI ---
st.set_page_config(page_title="VoidERP | Otonom Hazine", layout="wide")

st.markdown("""
    <style>
    .main { background-color: #0e1117; color: #ffffff; }
    .stMetric { background-color: #1a1c24; padding: 15px; border-radius: 10px; border: 1px solid #deff9a; }
    </style>
    """, unsafe_allow_html=True)

# --- SIDEBAR ---
st.sidebar.title("🛠️ Sistem Ayarları")
st.sidebar.markdown("---")
trust_threshold = st.sidebar.slider("Güven Eşiği (Trust Threshold)", 0.50, 0.95, 0.75, 0.05)
risk_tolerance = st.sidebar.slider("Risk Toleransı (ATR Multiplier)", 1.0, 5.0, 2.5, 0.5)
ece_limit = st.sidebar.slider("Maksimum ECE Hatası", 0.05, 0.30, 0.15, 0.01)

# --- ANA PANEL ---
st.title("🧿 VoidERP: Otonom Hazine Orkestrasyonu")
st.write(f"**Veri Kaynağı:** SAP S/4HANA Enterprise Management | **Durum:** Bağlı ✅")

# Üst Metrikler (Tutarlı hale getirildi)
m1, m2, m3, m4 = st.columns(4)
m1.metric("Toplam İşlem Hacmi", "₺1.240.000", "+12%")
m2.metric("Bloklanan Anomali", "42 Adet", "-5")
m3.metric("AI Güven Skoru (Ort.)", "%88", "+2%")
m4.metric("Tahmini Tasarruf", "₺24.800", "Lider") # 1.2M TL hacimde 24K TL tasarruf (yaklaşık %2) daha gerçekçidir.

st.markdown("---")

# --- GRAFİKLER ---
c1, c2 = st.columns([1, 1])

with c1:
    st.subheader("📊 SAP Veri Kalitesi & Anomali Dağılımı")
    sap_errors = pd.DataFrame({
        "Hata Tipi": ["Mükerrer Kayıt", "Büyük Tutar Sapması", "Eksik Metadata", "Geçersiz Para Birimi"],
        "Sıklık": [25, 15, 10, 5]
    })
    fig_sap = px.bar(sap_errors, x="Hata Tipi", y="Sıklık", color="Hata Tipi", 
                     template="plotly_dark", color_discrete_sequence=px.colors.qualitative.Pastel)
    st.plotly_chart(fig_sap, use_container_width=True)

with c2:
    st.subheader("🧠 AI Kalibrasyon Analizi (ECE vs. Accuracy)")
    conf = np.linspace(0.1, 1.0, 10)
    acc = conf - (ece_limit * np.random.rand(10))
    fig_ece = go.Figure()
    fig_ece.add_trace(go.Scatter(x=conf, y=conf, name="Mükemmel Kalibrasyon", line=dict(dash='dash', color='grey')))
    fig_ece.add_trace(go.Scatter(x=conf, y=acc, mode='lines+markers', name='Mevcut AI Performansı', line=dict(color='#deff9a')))
    fig_ece.update_layout(template="plotly_dark", xaxis_title="Güven Skoru", yaxis_title="Gerçek Doğruluk")
    st.plotly_chart(fig_ece, use_container_width=True)

# --- CANLI KARAR AKIŞI ---
st.subheader("⚡ Canlı Otonom Karar Akışı")
if 'logs' not in st.session_state:
    st.session_state.logs = []

if st.button("Sistemi Tetikle (Simülasyon Başlat)"):
    for i in range(5):
        trust = round(np.random.uniform(0.4, 0.98), 2)
        status = "ONAYLANDI" if trust > trust_threshold else "BLOKLANDI"
        reason = "ECE Güven Sınırı Geçildi" if status == "ONAYLANDI" else "Düşük Kalibre Güven"
        
        st.session_state.logs.insert(0, {
            "Zaman": datetime.now().strftime("%H:%M:%S"),
            "İşlem ID": f"SAP-{random.randint(1000,9999)}", # Artık 'random' tanımlı, hata vermez.
            "Tutar": f"₺{random.randint(500, 20000)}", # Para birimi ₺ olarak eşitlendi.
            "Durum": status,
            "Neden": reason,
            "AI Güven": f"%{int(trust*100)}"
        })
        time.sleep(0.5)

# Tabloyu renklendirme ve gösterme
df_logs = pd.DataFrame(st.session_state.logs)
if not df_logs.empty:
    def color_status(val):
        color = '#4ade80' if val == 'ONAYLANDI' else '#f87171'
        return f'color: {color}'
    
    st.table(df_logs) # Sade ve şık tablo görünümü
else:
    st.info("Veri akışı bekleniyor... Lütfen 'Sistemi Tetikle' butonuna basın.")

# --- VOIDCHAT: AJAN TABANLI FİNANSAL ASİSTAN ---
st.markdown("---")
st.subheader("💬 VoidChat: Otonom Finans Asistanı")

# Sohbet geçmişini başlat
if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": "Merhaba! Ben VoidERP finans asistanıyım. SAP verilerinizi ve nakit akışınızı analiz edebilirim. Size nasıl yardımcı olabilirim?"}
    ]

# Geçmiş mesajları ekrana bas
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "chart" in message:
            st.plotly_chart(message["chart"], use_container_width=True)

# Kullanıcı girişi
if prompt := st.chat_input("Örn: Gelecek ay maaş ödemelerim riskte mi?"):
    # 1. Kullanıcı mesajını göster ve kaydet
    st.chat_message("user").markdown(prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})

    # 2. Ajanların yanıt süreci
    with st.chat_message("assistant"):
        # Ajanların "Düşünme" Süreci (Expander içinde teknik derinlik)
        with st.status("Ajanlar senaryoyu tartışıyor...", expanded=True) as status:
            st.write("📡 **Data Pod:** SAP S/4HANA borç yaşlandırma tablosu çekiliyor...")
            time.sleep(1)
            st.write("🧠 **Intelligence Pod:** Nakit akış projeksiyonu ve gecikme olasılıkları hesaplanıyor...")
            time.sleep(1.2)
            st.write("🛡️ **Control Pod:** ECE güven skoru denetlendi. Tahmin geçerli.")
            status.update(label="Analiz Tamamlandı!", state="complete", expanded=False)

        # 3. Mantıksal Yanıt Üretimi
        response = ""
        chart_to_add = None

        # Basit keyword bazlı chatbot mantığı (Hackathon için güvenli liman)
        low_prompt = prompt.lower()
        if "maaş" in low_prompt or "kira" in low_prompt:
            response = "Analizime göre gelecek ayki ₺90.000 tutarındaki maaş ve kira yükümlülüklerinizi karşılayabiliyorsunuz. Kasadaki mevcut nakit ve beklenen ₺140.000 tahsilat güvenli bir marj bırakıyor."
            # Grafik üret
            x_data = ["Mevcut Nakit", "Beklenen Tahsilat", "Giderler (Maaş/Kira)"]
            y_data = [100000, 140000, -90000]
            fig = px.bar(x=x_data, y=y_data, color=x_data, template="plotly_dark", title="Nakit Pozisyonu Analizi")
            st.plotly_chart(fig, use_container_width=True)
            chart_to_add = fig
        
        elif "alacak" in low_prompt or "gecikme" in low_prompt:
            response = "Geciken alacaklar şu an nakit akışınızı %12 oranında riske atıyor. Özellikle ₺24.000 tutarındaki alacağın 15 günü geçmesi, ay sonu likidite oranınızı 1.1'e düşürebilir."
        
        else:
            response = "Bu konuda veriye dayalı bir analiz yapmamı ister misiniz? SAP verilerinizde yatırım karlılığı veya borç yaşlandırma analizi gerçekleştirebilirim."

        st.markdown(response)
        
        # Yanıtı geçmişe ekle
        new_msg = {"role": "assistant", "content": response}
        if chart_to_add:
            new_msg["chart"] = chart_to_add
        st.session_state.messages.append(new_msg)