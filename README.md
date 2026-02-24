
# ⚡ ETL Diário — Incrementos BT (Supabase + Power BI)

> Pipeline **simples e rápido** para carregar o *snapshot diário* de incrementos
> a partir de Excel recebido por e‑mail, publicar no **Supabase (Postgres)** e expor para o **Power BI** via *view*.

![Python](https://img.shields.io/badge/Python-3.12-2E3079?logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-Enabled-150458?logo=pandas&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.x-CA212A)
![Pytest](https://img.shields.io/badge/Tests-Pytest-0A9EDC?logo=pytest&logoColor=white)
![Supabase](https://img.shields.io/badge/Supabase-Postgres-3FCF8E?logo=supabase&logoColor=white)

---

## 🚀 Visão geral

Este projeto automatiza o carregamento **diário** de um Excel para a tabela `increment_bt` no
**Supabase**. A *view* `vw_increment_bt` combina esses incrementos com `general_reports` aplicando
regras de priorização (match por `INSTALACAO`, data e código), deixando tudo pronto para o **Power BI**.

**Pontos-chave**:
- 🧾 **Entrada**: um arquivo **.xlsx** na pasta `input/` (snapshot completo do dia).
- 🔄 **Transform**: tipagem, normalização e seleção de colunas necessárias.
- 🗃️ **Load seguro**: estratégia **`TRUNCATE + INSERT`** (sem *DROP TABLE*) — mantém a *view* funcionando.
- 🧪 **Testes**: suíte `pytest` com *mocks* e SQLite *in‑memory* compartilhado.

---

## 🧱 Arquitetura & pastas
```
.
├── etl/
│   ├── extract/
│   │   └── extract_excel.py        # Lê o primeiro .xlsx da pasta input
│   ├── transform/
│   │   └── transform_incremento.py # Seleciona/transforma colunas e tipos
│   └── load/
│       └── load_incremento.py      # Conecta no Supabase e faz TRUNCATE + INSERT
├── etl/main.py                      # Orquestra o pipeline (logs + barra de progresso)
├── input/                           # 📥 Coloque aqui o Excel diário
├── logs/                            # 🪵 Arquivos de log por execução
├── tests/                           # ✅ Pytest (extract/transform/load)
├── .gitignore                       # ❌ Arquivos que não sobem para o Github
├── .python-version                  # 🐍 Versão do python
├── pyproject.toml                   # 📝 Estrutura e dependências do projeto
├── poetry.lock                      # 📝 Estrutura e dependências do projeto
└── .env                             # 🔐 Variáveis de ambiente (SUPABASE_DB_URI)
```

---

## 🛠️ Como rodar localmente

### 1) Pré‑requisitos
- Python **3.12**
- Dependências do projeto instaladas (Poetry)
- Conta/projeto no **Supabase** com banco Postgres disponível

### 2) Variáveis de ambiente
Crie um arquivo `.env` na raiz do projeto com:
```bash
SUPABASE_DB_URI="postgresql+psycopg2://usuario:senha@host:porta/database"
```
> Dica: no Supabase, pegue a connection string **Postgres** (não a HTTP/REST). Use o *dialeto* `postgresql+psycopg2` para o SQLAlchemy.

### 3) Instalação
Usando `poetry` (exemplo):
```bash
poetry env use 3.12
source .venv/bin/activate   # Windows: .venv\Scripts\activate
poetry install
```

### 4) Execução do pipeline
Coloque o Excel do dia em `input/` e rode:
```bash
python -m etl.main
```
Saída esperada (resumo):
```
🚀 Iniciando pipeline ETL de incremento...
📂 [EXTRACT] ...
🔄 [TRANSFORM] ...
📤 [LOAD] ... ✅ [LOAD] N linhas carregadas na tabela increment_bt.
🏁 Pipeline ETL finalizado com sucesso!
```

Os logs detalhados ficam em `logs/etl_YYYY-MM-DD_HH-MM-SS.log`.

---

## 🔗 View no Supabase (Power BI‑ready)

A view `vw_increment_bt` faz o *join* entre `general_reports` (base de serviços realizados por equipe) e `increment_bt`
(apontamentos diários) usando prioridades de correspondência. Exemplo (ajuste `schema` se necessário):

```sql
CREATE OR REPLACE VIEW public.vw_increment_bt AS
SELECT
    g."UC / MD",
    g."STATUS",
    g."DATA_EXECUCAO",
    g."COD",
    g."EQUIPE",
    COALESCE(inc."INC_TOTAL", 0) AS "INC_TOTAL"
FROM public.general_reports g
LEFT JOIN LATERAL (
    SELECT i."INC_TOTAL"
    FROM public.increment_bt i
    WHERE i."INSTALACAO"::text = g."UC / MD"
      AND (
        (i."DATA_NOTA" = g."DATA_EXECUCAO" AND i."IRREGULARIDADE"::text = g."COD") OR
        (i."DATA_NOTA" = g."DATA_EXECUCAO") OR
        (
          EXTRACT(MONTH FROM i."DATA_NOTA") = EXTRACT(MONTH FROM g."DATA_EXECUCAO") AND
          EXTRACT(YEAR  FROM i."DATA_NOTA") = EXTRACT(YEAR  FROM g."DATA_EXECUCAO")
        )
      )
    ORDER BY CASE
        WHEN i."DATA_NOTA" = g."DATA_EXECUCAO" AND i."IRREGULARIDADE"::text = g."COD" THEN 1
        WHEN i."DATA_NOTA" = g."DATA_EXECUCAO" THEN 2
        WHEN EXTRACT(MONTH FROM i."DATA_NOTA") = EXTRACT(MONTH FROM g."DATA_EXECUCAO") AND
             EXTRACT(YEAR  FROM i."DATA_NOTA") = EXTRACT(YEAR  FROM g."DATA_EXECUCAO") THEN 3
        ELSE 4
    END
    LIMIT 1
) inc ON TRUE
WHERE
    g."STATUS" <> 'CANCELADO'
    AND g."REGIONAL" = 'SUL'
    AND g."GRUPO" = 'BT'
    AND g."DATA_EXECUCAO" >= DATE '2026-01-01';
```

> **Boas práticas**: sempre qualifique com `public.` (ou seu schema). Assim, o Power BI encontra a view
> de forma consistente entre ambientes.

---

## 🧪 Testes

Rode a suíte:
```bash
task test
```

### Notas sobre testes
- Para testar o *load* com SQLite *in‑memory*, usamos um **engine compartilhado** (`StaticPool`) e
  um *fallback* para **`DELETE FROM`** quando o dialeto não suporta `TRUNCATE`.
- O *load* agora **só limpa** a tabela se ela **já existir**, evitando erro no primeiro *run*.

---

## 🧩 Decisões de design

- **Idempotência sem DROP**: abandonar `if_exists='replace'` (que faz `DROP TABLE`) e adotar
  **`TRUNCATE + INSERT`**. Isso **preserva dependências** (ex.: a view) e é mais rápido.
- **Transação**: o `TRUNCATE` e os inserts ocorrem sob `engine.begin()`. Consumidores (ex.: Power BI)
  veem dados antigos até o *commit*; depois, veem o novo snapshot **sem janela vazia**.
- **Tipagem**: transformação garante tipos (`int`, `date`) e normaliza nulos em `IRREGULARIDADE`.

---

## 🧰 Referências técnicas rápidas

- **Entrada esperada (pós‑transform)** — colunas mínimas usadas no *load*:
  - `INSTALACAO` (int)
  - `DATA_NOTA` (date)
  - `DATA_BAIXA` (date)
  - `IRREGULARIDADE` (int, nulos viram 0)
  - `INC_TOTAL` (int)

- **Funções principais**
  - `etl.extract.extract_excel.ler_excel(pasta_input: str) -> pd.DataFrame`
  - `etl.transform.transform_incremento.transformar_incremento(df) -> pd.DataFrame`
  - `etl.load.load_incremento.carregar_incremento(df, engine, ..., replace_strategy="truncate-insert")`

---

## 🐛 Troubleshooting

- **Erro**: `cannot drop table increment_bt because other objects depend on it`  
  **Causa**: uso de `if_exists='replace'` (tenta `DROP TABLE`), mas a `vw_increment_bt` depende dela.  
  **Fix**: usar este projeto (estratégia `truncate-insert`) — não há `DROP`, logo a view permanece.

- **`sqlite3.OperationalError: no such table ...` nos testes**  
  **Causa**: tentar limpar antes de criar; bancos *in‑memory* sem `StaticPool`.  
  **Fix**: a função agora checa existência e os testes usam engine compartilhado.

---

## 📈 Roadmap
- [ ] Métricas de carga (tempo, linhas, throughput) nos logs
- [ ] Validações de esquema (contract tests) antes do load
- [ ] CI com workflow GitHub Actions (pytest + lint)
- [ ] Parametrizar `schema` via `.env`

---

## 👨🏻‍💻 Autor
Feito com energia por **Rômulo Barreto da Silva** — Analista de Distribuição Pleno⚡

> Dúvidas, ideias ou *PRs* são super bem‑vindos!
