"""Transformações do incremento (snapshot diário).

- Seleciona/normaliza colunas necessárias.
- Converte datas para datetime (DATE no banco).
- Converte inteiros para Int64 (nullable).
- Normaliza CLASSIFICACAO_IRREG a partir do mapa de códigos, quando possível.
- **Importante**: colunas dos meses (MES_01..MES_12) preservam NULL (não convertem vazio para 0).
"""

from __future__ import annotations
from typing import List
import pandas as pd

try:
    # Mantém fonte única, se existir no projeto
    from etl.shared.classificacao import MAPA_CLASSIFICACAO  # type: ignore
except Exception:
    # Fallback local (caso não exista o módulo compartilhado)
    MAPA_CLASSIFICACAO = {
        109: 'C100', 115: 'C100', 129: 'C100', 154: 'REGU',
        164: 'C100', 165: 'C100', 168: 'C100', 171: 'C100',
        172: 'C100', 174: 'C100', 175: 'REGU', 176: 'C100',
        188: 'C100', 201: 'C200', 202: 'C200', 203: 'C200',
        204: 'C200', 210: 'C200', 211: 'C200', 212: 'C200',
        214: 'C200', 215: 'C200', 221: 'C200', 300: 'ACAO',
        301: 'ACAO', 302: 'ACAO', 303: 'ACAO', 305: 'ACAO',
        306: 'ACAO', 309: 'ACAO', 312: 'ACAO', 314: 'ACAO',
        315: 'ACAO', 307: 'C300', 308: 'C300', 310: 'C300',
        311: 'C300',
    }

COLUNAS_NECESSARIAS: List[str] = [
    "GRUPO","PROJETO","CMPT","NOTA","INSTALACAO",
    "DATA_EXECUCAO","DATA_BAIXA","CLASSIFICACAO_IRREG","IRREGULARIDADE",
    "MES_01","MES_02","MES_03","MES_04","MES_05","MES_06",
    "MES_07","MES_08","MES_09","MES_10","MES_11","MES_12",
    "INC_TOTAL",
]

def transformar_incremento(df: pd.DataFrame) -> pd.DataFrame:
    colunas_faltando = [c for c in COLUNAS_NECESSARIAS if c not in df.columns]
    if colunas_faltando:
        raise KeyError(f"Colunas não encontradas no DataFrame: {colunas_faltando}")

    df = df[COLUNAS_NECESSARIAS].copy()

    # Datas (normaliza para meia-noite)
    df["DATA_EXECUCAO"] = pd.to_datetime(df["DATA_EXECUCAO"], errors="coerce").dt.normalize()
    df["DATA_BAIXA"]    = pd.to_datetime(df["DATA_BAIXA"],    errors="coerce").dt.normalize()

    # Inteiros (gerais) -> mantém comportamento atual com fillna(0) para estas colunas
    cols_int_gerais = ["INSTALACAO","CMPT","IRREGULARIDADE","INC_TOTAL"]
    for col in cols_int_gerais:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("Int64")

    # Meses -> **preservar NULL** (não usar fillna(0))
    meses = [f"MES_{i:02d}" for i in range(1, 13)]
    for col in meses:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        # Observação: valores vazios/strings não numéricas viram <NA> (NULL no banco)

    # Textos
    for col in ["GRUPO","PROJETO","NOTA","CLASSIFICACAO_IRREG"]:
        df[col] = df[col].astype(str).replace(["nan","None","NaN"], "").str.strip()

    # Normaliza classificação via mapa (quando possível)
    df["CLASSIFICACAO_IRREG"] = (
        df["IRREGULARIDADE"].map(MAPA_CLASSIFICACAO).fillna(df["CLASSIFICACAO_IRREG"])
    )

    print(f"✅ [TRANSFORM] {len(df)} linhas tratadas.")
    return df