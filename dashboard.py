import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="Análise de Risco Contratos Ceará Transparente",
    layout="wide"
)

# =========================
# CARREGAMENTO
# =========================
@st.cache_data(ttl=300)
def carregar_dados():
    df = pd.read_csv("anomalias_contratos.csv")

    df["data_assinatura"] = pd.to_datetime(df["data_assinatura"], errors="coerce")
    df["detectado_em"] = pd.to_datetime(df["detectado_em"], errors="coerce")

    return df


st.title("Análise de risco dos contratos do Ceará Transparente")
st.markdown("Análise de contratos com indicadores de risco e anomalia.")

try:
    df = carregar_dados()
except Exception as e:
    st.error("Erro ao carregar os dados.")
    st.exception(e)
    st.stop()

if df.empty:
    st.warning("A tabela está vazia.")
    st.stop()

# =========================
# SIDEBAR - FILTROS
# =========================
st.sidebar.header("Filtros")

# Risco
niveis_risco = sorted(df["nivel_risco"].dropna().astype(str).unique().tolist())

risco_sel = st.sidebar.multiselect(
    "Nível de risco",
    options=niveis_risco,
    default=niveis_risco
)

# Valor
valor_min = float(df["valor_global"].fillna(0).min())
valor_max = float(df["valor_global"].fillna(0).max())

faixa_valor = st.sidebar.slider(
    "Faixa de valor global",
    min_value=valor_min,
    max_value=valor_max,
    value=(valor_min, valor_max)
)

# Datas (corrigido)
data_min = df["data_assinatura"].min()
data_max = df["data_assinatura"].max()

if pd.notna(data_min) and pd.notna(data_max):
    periodo = st.sidebar.date_input(
        "Período de assinatura",
        value=(data_min.date(), data_max.date())
    )
else:
    st.sidebar.warning("Datas inválidas na base.")
    periodo = None

# =========================
# FILTRAGEM
# =========================
df_filtrado = df.copy()

df_filtrado = df_filtrado[
    df_filtrado["nivel_risco"].astype(str).isin(risco_sel)
]

df_filtrado = df_filtrado[
    df_filtrado["valor_global"].fillna(0).between(faixa_valor[0], faixa_valor[1])
]

if periodo and isinstance(periodo, tuple) and len(periodo) == 2:
    data_inicio = pd.to_datetime(periodo[0])
    data_fim = pd.to_datetime(periodo[1])

    df_filtrado = df_filtrado[
        df_filtrado["data_assinatura"].between(data_inicio, data_fim)
    ]

# =========================
# MÉTRICAS
# =========================
total_registros = len(df_filtrado)
valor_total = df_filtrado["valor_global"].fillna(0).sum()
risco_alto = (df_filtrado["nivel_risco"].astype(str).str.upper() == "ALTO").sum()
score_medio = df_filtrado["score_anomalia"].mean()

c1, c2, c3, c4 = st.columns(4)

c1.metric("Total de registros", f"{total_registros:,}".replace(",", "."))
c2.metric(
    "Valor total",
    f"R$ {valor_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
)
c3.metric("Qtd. risco alto", int(risco_alto))
c4.metric(
    "Score médio",
    f"{score_medio:.4f}" if pd.notna(score_medio) else "N/A"
)

# =========================
# GRÁFICOS
# =========================
col1, col2 = st.columns(2)

# Gráfico 1
with col1:
    st.subheader("Distribuição por nível de risco")

    risco_count = (
        df_filtrado["nivel_risco"]
        .fillna("NÃO INFORMADO")
        .value_counts()
        .reset_index()
    )
    risco_count.columns = ["nivel_risco", "quantidade"]

    fig_risco = px.bar(
        risco_count,
        x="nivel_risco",
        y="quantidade",
        title="Quantidade por nível de risco"
    )

    st.plotly_chart(fig_risco, use_container_width=True)

# Gráfico 2 (fornecedor corrigido)
with col2:
    st.subheader("Top 10 fornecedores por valor global")

    top_fornecedores = (
        df_filtrado.groupby("fornecedor_nome", as_index=False)["valor_global"]
        .sum()
        .sort_values("valor_global", ascending=False)
        .head(10)
    )

    top_fornecedores["fornecedor_curto"] = top_fornecedores["fornecedor_nome"].apply(
        lambda x: " ".join(str(x).split()[:2])
    )

    fig_fornecedores = px.bar(
        top_fornecedores,
        x="fornecedor_curto",
        y="valor_global",
        hover_data={"fornecedor_nome": True},
        title="Fornecedores com maior valor global"
    )

    fig_fornecedores.update_layout(xaxis_tickangle=-20)

    st.plotly_chart(fig_fornecedores, use_container_width=True)

# =========================
# LINHA 2 DE GRÁFICOS
# =========================
col3, col4 = st.columns(2)

# Série temporal
with col3:
    st.subheader("Evolução por data de assinatura")

    serie_tempo = (
        df_filtrado.dropna(subset=["data_assinatura"])
        .groupby("data_assinatura", as_index=False)["valor_global"]
        .sum()
        .sort_values("data_assinatura")
    )

    fig_tempo = px.line(
        serie_tempo,
        x="data_assinatura",
        y="valor_global",
        title="Valor global ao longo do tempo"
    )

    st.plotly_chart(fig_tempo, use_container_width=True)

# Scatter
with col4:
    st.subheader("Relação entre risco e valor")

    df_scatter = df_filtrado.dropna(
        subset=["percentil_risco", "valor_global", "nivel_risco"]
    )

    fig_scatter = px.scatter(
        df_scatter,
        x="percentil_risco",
        y="valor_global",
        color="nivel_risco",
        hover_data=["fornecedor_nome", "objeto"],
        title="Percentil de risco x Valor global",
        color_discrete_map={
            "ALTO": "#e74c3c",     # vermelho
            "MÉDIO": "#f1c40f",    # amarelo
            "BAIXO": "#2ecc71"     # verde
        }
    )

    st.plotly_chart(fig_scatter, use_container_width=True)

# =========================
# TABELA
# =========================
st.subheader("Dados detalhados")

colunas_exibir = [
    "id",
    "isn_sic",
    "objeto",
    "fornecedor_nome",
    "orgao_nome",
    "valor_global",
    "prazo_vigencia_dias",
    "score_anomalia",
    "percentil_risco",
    "nivel_risco",
    "data_assinatura",
    "detectado_em"
]

st.dataframe(
    df_filtrado[colunas_exibir],
    use_container_width=True,
    hide_index=True
)