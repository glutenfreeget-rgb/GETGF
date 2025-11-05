
import sys
import time
from datetime import datetime, date, timedelta

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Streamlit - Diagnóstico Mínimo", layout="wide", initial_sidebar_state="expanded")

st.title("✅ Teste rápido do Streamlit")
st.caption("Se você está vendo esta página, o Streamlit subiu corretamente. Use os blocos abaixo para testar widgets, cache, upload e renderização.")

# ---- Versões & ambiente ----
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Python", sys.version.split()[0])
with col2:
    try:
        import streamlit as _st
        st.metric("Streamlit", _st.__version__)
    except Exception as e:
        st.error(f"Falha ao obter versão do Streamlit: {e}")
with col3:
    st.metric("Hora do servidor", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

st.divider()

# ---- Widgets básicos ----
st.subheader("1) Widgets básicos")
name = st.text_input("Seu nome", value="Claudia")
num = st.number_input("Um número", min_value=0, max_value=100, value=7, step=1)
ok = st.button("Clique aqui")
if ok:
    st.success(f"Olá, {name}! Você clicou. Número = {num}")

st.divider()

# ---- Form + submit ----
st.subheader("2) Formulário com submit")
with st.form("demo_form"):
    d = st.date_input("Data", value=date.today())
    v = st.number_input("Valor R$", min_value=0.0, value=12.34, step=0.01, format="%.2f")
    envio = st.form_submit_button("Enviar")
if envio:
    st.info(f"Form enviado: {d} • {v:.2f}")

st.divider()

# ---- Cache de dados ----
st.subheader("3) Cache de dados (st.cache_data)")
@st.cache_data
def soma_lenta(x: int, y: int):
    time.sleep(1.0)  # simula processamento pesado
    return x + y

a, b = st.columns(2)
with a:
    a1 = st.slider("A", 0, 50, 10)
with b:
    b1 = st.slider("B", 0, 50, 20)
st.write("Resultado (com cache de 1s na 1ª vez):", soma_lenta(a1, b1))

st.divider()

# ---- Tabela & gráfico nativos ----
st.subheader("4) Tabela e gráfico nativos")
df = pd.DataFrame({
    "dia": pd.date_range(date.today() - timedelta(days=9), periods=10),
    "vendas": [max(0, 50 + i*3 - (i%4)*5) for i in range(10)]
})
st.dataframe(df, use_container_width=True, hide_index=True)
st.line_chart(df.set_index("dia"))

st.divider()

# ---- Upload de CSV ----
st.subheader("5) Upload e leitura de CSV")
up = st.file_uploader("Selecione um CSV", type=["csv"])
if up is not None:
    try:
        df_up = pd.read_csv(up)
        st.success(f"Arquivo lido: {df_up.shape[0]} linhas x {df_up.shape[1]} colunas")
        st.dataframe(df_up.head(50), use_container_width=True)
    except Exception as e:
        st.error(f"Falha ao ler CSV: {e}")

st.divider()

# ---- Secrets visíveis? (apenas chaves) ----
st.subheader("6) Secrets detectados (somente nomes das chaves)")
try:
    keys = list(st.secrets._secrets.keys()) if hasattr(st.secrets, "_secrets") else list(st.secrets.keys())
    if keys:
        st.write("Chaves encontradas:", ", ".join(keys))
    else:
        st.write("Nenhuma chave em secrets.")
except Exception as e:
    st.write(f"Não foi possível inspecionar secrets: {e}")
