"""
================================================================================
DAG: anomalias_contratos_ceara
Prof. Daniel Teófilo
aluno: Carlos Arthur
================================================================================

Pipeline ETL:
  1. extrair_contratos
  2. salvar_postgres
  3. detectar_anomalias
  4. salvar_anomalias
  5. gerar_csv_anomalias
  6. enviar_email_atualizacao
================================================================================
"""

import logging
import json
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import requests
from psycopg2.extras import execute_values
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÕES GLOBAIS
# ─────────────────────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host": "",  # ajuste conforme seu ambiente
    "port": "",
    "database": "",
    "user": "",
    "password": "",
}

API_BASE_URL = (
    "https://api-dados-abertos.cearatransparente.ce.gov.br"
    "/transparencia/contratos/contratos"
)

DIAS_RETROATIVOS = 365
PERIODO_ANALISE_DIAS = 365
CONTAMINACAO = 0.05

EMAIL_DESTINO = [""]
DASHBOARD_URL = "https://dashcontratosrisco-qyhjq37nz3w7hhwpknfpez.streamlit.app/"

# ─────────────────────────────────────────────────────────────────────────────
# FUNÇÕES AUXILIARES
# ─────────────────────────────────────────────────────────────────────────────

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_temp_contracts_file_path():
    dag_dir = Path(__file__).resolve().parent
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return dag_dir / f"contratos_tmp_{timestamp}.jsonl"


def get_temp_anomalias_csv_path():
    dag_dir = Path(__file__).resolve().parent
    return dag_dir / "anomalias_contratos_dashboard.csv"


def criar_tabelas_se_nao_existirem():
    ddl_contratos = """
        CREATE TABLE IF NOT EXISTS contratos (
            id                   BIGSERIAL PRIMARY KEY,
            isn_sic              TEXT,
            objeto               TEXT,
            fornecedor_nome      TEXT,
            fornecedor_cnpj      TEXT,
            orgao_nome           TEXT,
            modalidade           TEXT,
            valor_inicial        NUMERIC(18, 2),
            valor_global         NUMERIC(18, 2),
            data_assinatura      DATE,
            data_inicio_vigencia DATE,
            data_fim_vigencia    DATE,
            prazo_vigencia_dias  INTEGER,
            json_original        JSONB,
            inserido_em          TIMESTAMP DEFAULT NOW(),
            UNIQUE(isn_sic, data_assinatura)
        );
    """

    ddl_anomalias = """
        CREATE TABLE IF NOT EXISTS anomalias_contratos (
            id                  BIGSERIAL PRIMARY KEY,
            isn_sic             TEXT,
            objeto              TEXT,
            fornecedor_nome     TEXT,
            orgao_nome          TEXT,
            valor_global        NUMERIC(18, 2),
            prazo_vigencia_dias INTEGER,
            score_anomalia      NUMERIC(10, 6),
            percentil_risco     INTEGER,
            nivel_risco         TEXT,
            data_assinatura     DATE,
            detectado_em        TIMESTAMP DEFAULT NOW()
        );
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl_contratos)
            cur.execute(ddl_anomalias)
        conn.commit()

    logger.info("Tabelas verificadas/criadas com sucesso.")


def formatar_moeda_br(valor):
    if valor is None:
        return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# ─────────────────────────────────────────────────────────────────────────────
# TASK 1 — EXTRAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

def extrair_contratos(**context):
    hoje = datetime.now()
    data_fim = hoje.strftime("%d/%m/%Y")
    data_inicio = (hoje - timedelta(days=DIAS_RETROATIVOS)).strftime("%d/%m/%Y")

    logger.info(f"Buscando contratos de {data_inicio} até {data_fim}")

    arquivo_temporario = get_temp_contracts_file_path()
    total_contratos = 0
    pagina_atual = 1
    total_paginas = None

    with arquivo_temporario.open("w", encoding="utf-8") as arquivo_saida:
        while True:
            params = {
                "page": pagina_atual,
                "data_assinatura_inicio": data_inicio,
                "data_assinatura_fim": data_fim,
            }

            try:
                response = requests.get(
                    API_BASE_URL,
                    params=params,
                    timeout=30,
                )
                response.raise_for_status()

            except requests.exceptions.Timeout:
                logger.error(f"Timeout na página {pagina_atual}. Encerrando extração.")
                break
            except requests.exceptions.HTTPError as e:
                logger.error(f"Erro HTTP {e.response.status_code} na página {pagina_atual}: {e}")
                break

            dados = response.json()
            registros = dados.get("data", [])
            meta = dados.get("sumary", {})

            if total_paginas is None:
                total_paginas = meta.get("total_pages", 1)
                total_registros = meta.get("total_records", 0)
                logger.info(
                    f"Total de registros: {total_registros} | "
                    f"Total de páginas: {total_paginas}"
                )

            for registro in registros:
                arquivo_saida.write(json.dumps(registro, ensure_ascii=False) + "\n")

            total_contratos += len(registros)
            logger.info(
                f"Página {pagina_atual}/{total_paginas} — "
                f"{len(registros)} registros coletados"
            )

            if pagina_atual >= total_paginas or not registros:
                break

            pagina_atual += 1

    logger.info(
        f"Extração concluída: {total_contratos} contratos no total. "
        f"Arquivo temporário: {arquivo_temporario}"
    )

    context["ti"].xcom_push(key="contratos_arquivo", value=str(arquivo_temporario))
    return total_contratos


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2 — ARMAZENAMENTO
# ─────────────────────────────────────────────────────────────────────────────

def salvar_postgres(**context):
    criar_tabelas_se_nao_existirem()

    ti = context["ti"]
    arquivo_temporario = ti.xcom_pull(task_ids="extrair_contratos", key="contratos_arquivo")

    if not arquivo_temporario:
        logger.warning("Nenhum arquivo temporário recebido para salvar.")
        return 0

    caminho_arquivo = Path(arquivo_temporario)

    if not caminho_arquivo.exists():
        logger.warning(f"Arquivo temporário não encontrado: {caminho_arquivo}")
        return 0

    def parse_data(valor):
        if not valor:
            return None
        s = str(valor)
        if "T" in s:
            s = s.split("T")[0]
        for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
            try:
                d = datetime.strptime(s, fmt).date()
                if d.year < 1900:
                    return None
                return d
            except ValueError:
                continue
        return None

    def parse_valor(v):
        if v is None:
            return None
        try:
            return float(str(v).replace("R$", "").replace(".", "").replace(",", ".").strip())
        except (ValueError, AttributeError):
            return None

    registros = []
    try:
        with caminho_arquivo.open("r", encoding="utf-8") as arquivo_entrada:
            for linha in arquivo_entrada:
                linha = linha.strip()
                if not linha:
                    continue

                c = json.loads(linha)
                data_assinatura = parse_data(c.get("data_assinatura"))
                data_inicio = parse_data(c.get("data_inicio"))
                data_fim = parse_data(c.get("data_termino"))

                prazo_dias = None
                if data_inicio and data_fim and data_fim > data_inicio:
                    prazo_dias = (data_fim - data_inicio).days

                valor_inicial = parse_valor(c.get("valor_contrato"))
                valor_global = parse_valor(
                    c.get("valor_atualizado_concedente") or c.get("valor_contrato")
                )

                registros.append((
                    c.get("isn_sic"),
                    c.get("descricao_objeto"),
                    c.get("descricao_nome_credor"),
                    c.get("plain_cpf_cnpj_financiador") or c.get("cpf_cnpj_financiador"),
                    c.get("cod_orgao"),
                    c.get("descricao_modalidade"),
                    valor_inicial,
                    valor_global,
                    data_assinatura,
                    data_inicio,
                    data_fim,
                    prazo_dias,
                    json.dumps(c, ensure_ascii=False),
                ))

        if not registros:
            logger.warning("Arquivo temporário sem contratos para salvar.")
            return 0

        sql = """
            INSERT INTO contratos (
                isn_sic, objeto, fornecedor_nome, fornecedor_cnpj,
                orgao_nome, modalidade, valor_inicial, valor_global,
                data_assinatura, data_inicio_vigencia, data_fim_vigencia,
                prazo_vigencia_dias, json_original
            ) VALUES %s
            ON CONFLICT (isn_sic, data_assinatura) DO NOTHING
        """

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, registros, page_size=500)
                inseridos = cur.rowcount
            conn.commit()

        logger.info(
            f"{len(registros)} contratos processados | "
            f"{inseridos} novos inseridos no PostgreSQL."
        )

        return inseridos
    finally:
        if caminho_arquivo.exists():
            caminho_arquivo.unlink()
            logger.info(f"Arquivo temporário removido: {caminho_arquivo}")


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3 — DETECÇÃO DE ANOMALIAS
# ─────────────────────────────────────────────────────────────────────────────

def detectar_anomalias(**context):
    logger.info("Carregando contratos do PostgreSQL para detecção de anomalias...")

    sql_leitura = """
        SELECT
            id,
            isn_sic,
            objeto,
            fornecedor_nome,
            orgao_nome,
            valor_global,
            valor_inicial,
            prazo_vigencia_dias,
            data_assinatura,
            modalidade
        FROM contratos
        WHERE
            data_assinatura >= CURRENT_DATE - INTERVAL '%s days'
            AND valor_global IS NOT NULL
            AND valor_global > 0
        ORDER BY data_assinatura DESC
    """ % PERIODO_ANALISE_DIAS

    with get_db_connection() as conn:
        df = pd.read_sql(sql_leitura, conn)

    logger.info(f"Contratos carregados para análise: {len(df)} registros")

    if len(df) < 10:
        logger.warning(
            "Poucos contratos para treinar o modelo (mínimo recomendado: 10). "
            "Abortando detecção."
        )
        return 0

    df["valor_por_dia"] = df.apply(
        lambda row: row["valor_global"] / row["prazo_vigencia_dias"]
        if row["prazo_vigencia_dias"] and row["prazo_vigencia_dias"] > 0
        else row["valor_global"],
        axis=1,
    )

    df["log_valor_global"] = np.log1p(df["valor_global"])
    df["log_valor_por_dia"] = np.log1p(df["valor_por_dia"])

    features_modelo = [
        "log_valor_global",
        "log_valor_por_dia",
        "prazo_vigencia_dias",
    ]

    df_modelo = df[features_modelo + ["id", "isn_sic"]].dropna()
    X = df_modelo[features_modelo].values

    logger.info(
        f"Features: {features_modelo} | "
        f"Contratos válidos para o modelo: {len(X)}"
    )

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    modelo = IsolationForest(
        n_estimators=200,
        contamination=CONTAMINACAO,
        random_state=42,
        n_jobs=-1,
    )
    modelo.fit(X_scaled)

    df_modelo = df_modelo.copy()
    df_modelo["predicao"] = modelo.predict(X_scaled)
    df_modelo["score_anomalia"] = modelo.score_samples(X_scaled)

    df_resultado = df_modelo.merge(
        df[["id", "objeto", "fornecedor_nome", "orgao_nome",
            "valor_global", "data_assinatura"]],
        on="id",
        how="left",
    )

    df_anomalias = df_resultado[df_resultado["predicao"] == -1].copy()

    scores_invertidos = -df_anomalias["score_anomalia"]
    df_anomalias["percentil_risco"] = (
        scores_invertidos.rank(pct=True) * 100
    ).astype(int)

    def classificar_risco(percentil):
        if percentil >= 90:
            return "ALTO"
        elif percentil >= 70:
            return "MÉDIO"
        else:
            return "BAIXO"

    df_anomalias["nivel_risco"] = df_anomalias["percentil_risco"].apply(classificar_risco)

    qtd_anomalias = len(df_anomalias)
    logger.info(
        f"Anomalias detectadas: {qtd_anomalias} de {len(df_modelo)} contratos analisados "
        f"({qtd_anomalias/len(df_modelo)*100:.1f}%)"
    )

    context["ti"].xcom_push(
        key="anomalias",
        value=df_anomalias.to_dict(orient="records"),
    )

    return qtd_anomalias


# ─────────────────────────────────────────────────────────────────────────────
# TASK 4 — SALVAR ANOMALIAS
# ─────────────────────────────────────────────────────────────────────────────

def salvar_anomalias(**context):
    ti = context["ti"]
    anomalias = ti.xcom_pull(task_ids="detectar_anomalias", key="anomalias")

    if not anomalias:
        logger.info("Nenhuma anomalia para salvar.")
        return 0

    registros = [
        (
            a.get("isn_sic"),
            a.get("objeto"),
            a.get("fornecedor_nome"),
            a.get("orgao_nome"),
            a.get("valor_global"),
            a.get("prazo_vigencia_dias"),
            float(a.get("score_anomalia", 0)),
            int(a.get("percentil_risco", 0)),
            a.get("nivel_risco"),
            a.get("data_assinatura"),
        )
        for a in anomalias
    ]

    sql_truncate = "TRUNCATE TABLE anomalias_contratos RESTART IDENTITY;"

    sql_insert = """
        INSERT INTO anomalias_contratos (
            isn_sic, objeto, fornecedor_nome, orgao_nome,
            valor_global, prazo_vigencia_dias, score_anomalia,
            percentil_risco, nivel_risco, data_assinatura
        ) VALUES %s
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_truncate)
            execute_values(cur, sql_insert, registros)
        conn.commit()

    df = pd.DataFrame(anomalias)
    resumo = df["nivel_risco"].value_counts().to_dict() if "nivel_risco" in df else {}
    logger.info(
        f"{len(registros)} anomalias salvas. "
        f"Resumo por risco: ALTO={resumo.get('ALTO', 0)}, "
        f"MÉDIO={resumo.get('MÉDIO', 0)}, "
        f"BAIXO={resumo.get('BAIXO', 0)}"
    )

    return len(registros)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 5 — GERAR CSV E RESUMO
# ─────────────────────────────────────────────────────────────────────────────

def gerar_csv_anomalias(**context):
    """
    Gera CSV local para anexo e monta resumo do e-mail.
    """
    caminho_csv = get_temp_anomalias_csv_path()

    query = """
        SELECT
            id,
            isn_sic,
            objeto,
            fornecedor_nome,
            orgao_nome,
            valor_global,
            prazo_vigencia_dias,
            score_anomalia,
            percentil_risco,
            nivel_risco,
            data_assinatura,
            detectado_em
        FROM anomalias_contratos
        ORDER BY percentil_risco DESC, score_anomalia ASC
    """

    with get_db_connection() as conn:
        df = pd.read_sql(query, conn)

    df.to_csv(caminho_csv, index=False, encoding="utf-8-sig")
    logger.info(f"CSV de anomalias gerado em: {caminho_csv}")

    total_anomalias = len(df)
    qtd_alto = int((df["nivel_risco"] == "ALTO").sum()) if "nivel_risco" in df.columns else 0
    qtd_medio = int((df["nivel_risco"] == "MÉDIO").sum()) if "nivel_risco" in df.columns else 0
    qtd_baixo = int((df["nivel_risco"] == "BAIXO").sum()) if "nivel_risco" in df.columns else 0
    valor_total = float(df["valor_global"].fillna(0).sum()) if "valor_global" in df.columns else 0.0

    data_execucao = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    html_resumo = f"""
    <h3>Atualização concluída com sucesso</h3>

    <p>Os dados do dashboard de contratos com risco foram atualizados.</p>

    <p>
        <b>Dashboard:</b>
        <a href="{DASHBOARD_URL}">
            {DASHBOARD_URL}
        </a>
    </p>

    <h4>Resumo da execução</h4>
    <ul>
        <li><b>Data/hora da atualização:</b> {data_execucao}</li>
        <li><b>Total de anomalias detectadas:</b> {total_anomalias}</li>
        <li><b>Risco ALTO:</b> {qtd_alto}</li>
        <li><b>Risco MÉDIO:</b> {qtd_medio}</li>
        <li><b>Risco BAIXO:</b> {qtd_baixo}</li>
        <li><b>Valor total anômalo:</b> {formatar_moeda_br(valor_total)}</li>
    </ul>

    <p>O arquivo CSV com as anomalias detectadas segue em anexo.</p>

    <p>Atenciosamente,<br>Pipeline Airflow</p>
    """

    context["ti"].xcom_push(key="csv_anomalias_path", value=str(caminho_csv))
    context["ti"].xcom_push(key="email_html_resumo", value=html_resumo)

    return str(caminho_csv)


# ─────────────────────────────────────────────────────────────────────────────
# DEFINIÇÃO DA DAG
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="anomalias_contratos_ceara_v5",
    description=(
        "Pipeline diário: extrai contratos da API do Ceará Transparente, "
        "armazena no PostgreSQL, detecta anomalias financeiras, "
        "salva o resultado e envia e-mail com resumo e CSV anexo."
    ),
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["contratos", "anomalias", "ceara-transparente", "ml", "email"],
    default_args={
        "owner": "daniel_teofilo",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "email_on_retry": False,
    },
) as dag:

    task_extrair = PythonOperator(
        task_id="extrair_contratos",
        python_callable=extrair_contratos,
    )

    task_salvar = PythonOperator(
        task_id="salvar_postgres",
        python_callable=salvar_postgres,
    )

    task_anomalias = PythonOperator(
        task_id="detectar_anomalias",
        python_callable=detectar_anomalias,
    )

    task_salvar_anomalias = PythonOperator(
        task_id="salvar_anomalias",
        python_callable=salvar_anomalias,
    )

    task_gerar_csv = PythonOperator(
        task_id="gerar_csv_anomalias",
        python_callable=gerar_csv_anomalias,
    )

    task_enviar_email = EmailOperator(
        task_id="enviar_email_atualizacao",
        to=EMAIL_DESTINO,
        subject="Atualização diária - Dashboard de Anomalias em Contratos",
        html_content="{{ ti.xcom_pull(task_ids='gerar_csv_anomalias', key='email_html_resumo') }}",
        files=[str(get_temp_anomalias_csv_path())],
    )

    task_extrair >> task_salvar >> task_anomalias >> task_salvar_anomalias >> task_gerar_csv >> task_enviar_email