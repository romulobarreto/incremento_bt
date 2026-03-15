"""Extração do Supabase: recorte mínimo da tabela general_reports.

- Filtra "GRUPO" = 'BT' (sem filtrar Regional).
- Sanitiza UC/instalação (mantém só dígitos, remove zeros à esquerda).
- Ordena DATA_EXECUCAO DESC para priorizar o mais recente nos merges.
"""


import pandas as pd
from sqlalchemy import Engine, text

def _to_nullable_int(series: pd.Series) -> pd.Series:
    """Converte para Int64 (nullable) preservando apenas valores inteiros.
    Valores não-inteiros viram NA.
    """
    s = pd.to_numeric(series, errors="coerce")          # -> float64 com NaN
    s = s.astype("Float64")                             # aceita pd.NA
    mask_int = s.isna() | ((s % 1).abs() < 1e-9)        # inteiro "de verdade"
    s = s.where(mask_int, pd.NA).round(0)               # zera frações que restaram
    return s.astype("Int64")                            # -> Int64 (nullable)

def _sanear_instalacao(series: pd.Series) -> pd.Series:
    s = (
        series.astype(str)
        .str.strip()
        .str.replace(r"\D", "", regex=True)  # mantém só dígitos
        .str.lstrip("0")
    )
    # instalacao deve ser inteiro; usa o mesmo helper
    return _to_nullable_int(s)

def ler_general_reports(engine: Engine) -> pd.DataFrame:
    query = text("""
        SELECT
            "UC / MD"       AS instalacao,
            "COD"           AS irregularidade,
            "DATA_EXECUCAO" AS data_exec_rep,
            "DATA BAIXADO"  AS data_baixa_rep,
            "EQUIPE"        AS equipe
        FROM general_reports
        WHERE "GRUPO" = 'BT'
        ORDER BY "DATA_EXECUCAO" DESC NULLS LAST
    """)

    df = pd.read_sql(query, engine)

    # Tipos básicos + sanitização
    df["instalacao"]     = _sanear_instalacao(df["instalacao"])         
    df["irregularidade"] = _to_nullable_int(df["irregularidade"])        
    df["data_exec_rep"]  = pd.to_datetime(df["data_exec_rep"], errors="coerce")
    df["data_baixa_rep"] = pd.to_datetime(df["data_baixa_rep"], errors="coerce")

    # Remove linhas sem instalação (chave obrigatória)
    df = df[df["instalacao"].notna()].copy()

    print(f"✅ [EXTRACT DB] {len(df)} registros carregados da general_reports.")
    return df
