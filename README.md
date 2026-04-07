# 🚨 Detecção de Anomalias em Contratos Públicos — Ceará Transparente

## 📌 Descrição

Este projeto implementa um pipeline completo de engenharia de dados com foco em detecção de anomalias em contratos públicos do Ceará Transparente.

O sistema realiza a extração automatizada dos contratos via API pública, armazena os dados em banco PostgreSQL, aplica técnicas de Machine Learning para identificar padrões anômalos e disponibiliza os resultados em um dashboard interativo desenvolvido em Streamlit.

🔗 **Acesse o dashboard:**  
https://dashcontratosrisco-qyhjq37nz3w7hhwpknfpez.streamlit.app/

---

## 🏗️ Arquitetura do Pipeline

API Ceará Transparente  
        ↓  
Airflow (Orquestração)  
        ↓  
PostgreSQL (Armazenamento)  
        ↓  
Detecção de Anomalias (Machine Learning)  
        ↓  
Tabela de Anomalias  
        ↓  
Dashboard (Streamlit)  
        ↓  
Notificação por Email  

---

## ⚙️ Tecnologias Utilizadas

- 🐍 Python  
- 🔄 Apache Airflow  
- 🗄️ PostgreSQL  
- 📊 Pandas / NumPy  
- 🤖 Scikit-learn (Isolation Forest)  
- 🌐 Requests  
- 📧 EmailOperator (Airflow)  
- 📊 Streamlit  
- 📈 Plotly  

---

## 🔄 Pipeline de Dados

### 1. 📥 Extração de Dados

Arquivo: dag_anomalias_contratos_v4.py  

- Consumo da API de contratos públicos  
- Paginação automática  
- Armazenamento temporário em JSON  
- Coleta de dados históricos  

---

### 2. 🗃️ Armazenamento

- Inserção em banco PostgreSQL  
- Criação automática de tabelas:
  - `contratos`
  - `anomalias_contratos`
- Controle de duplicidade com UNIQUE  

---

### 3. 🤖 Detecção de Anomalias

- Modelo utilizado: **Isolation Forest**
- Features analisadas:
  - Valor global  
  - Valor por dia  
  - Prazo do contrato  

- Etapas:
  - Normalização dos dados (StandardScaler)
  - Aplicação do modelo
  - Geração de score de anomalia
  - Classificação de risco:
    - 🔴 Alto
    - 🟡 Médio
    - 🟢 Baixo

---

### 4. 📊 Geração de Dados Analíticos

- Criação de tabela `anomalias_contratos`
- Cálculo de percentil de risco
- Geração de CSV para consumo do dashboard

---

### 5. 📧 Notificação Automática

- Envio de e-mail após execução
- Contém:
  - Resumo da execução
  - Quantidade de anomalias
  - Link do dashboard
  - CSV em anexo  

---

### 6. 📊 Dashboard Interativo

Arquivo: dashboard.py  

- Desenvolvido com Streamlit
- Visualizações:
  - Distribuição por nível de risco
  - Top fornecedores por valor
  - Série temporal
  - Scatter (risco x valor)
- Filtros interativos:
  - Nível de risco
  - Faixa de valor
  - Período de assinatura

---

## 🚀 Funcionalidades

- Pipeline automatizado com Airflow  
- Detecção de anomalias com Machine Learning  
- Classificação de risco de contratos  
- Dashboard interativo em tempo real  
- Notificação automática por e-mail  

---

## 📂 Estrutura do Projeto

projeto/  
├── dag_anomalias_contratos_v4.py   # Pipeline Airflow  
├── dashboard.py                   # Dashboard Streamlit  
├── anomalias_contratos.csv        # Dados para visualização  
└── README.md  

---

## ▶️ Como Executar

### Airflow (Pipeline)

- Configurar conexão com PostgreSQL  
- Adicionar DAG na pasta do Airflow  
- Executar DAG manualmente ou agendada  

### Dashboard

```bash
streamlit run dashboard.py
```

---

## 📈 Insights Possíveis

- Identificação de contratos com valores atípicos  
- Análise de fornecedores com maior risco  
- Monitoramento de gastos públicos  
- Detecção de padrões suspeitos  

---

## 🧠 Aprendizados

- Orquestração de pipelines com Airflow  
- Aplicação de Machine Learning em dados reais  
- Detecção de anomalias  
- Desenvolvimento de dashboards interativos  
- Integração entre ETL + ML + BI  

---

## ⭐ Diferenciais

- Uso de Machine Learning em dados públicos reais  
- Pipeline automatizado ponta a ponta  
- Integração completa: API → Banco → ML → Dashboard  
- Visualização interativa com atualização automática  

