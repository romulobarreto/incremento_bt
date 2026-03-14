"""Módulo responsável pela extração de dados do banco de dados Supabase."""

import pandas as pd
from sqlalchemy import Engine, text


def ler_general_reports(engine: Engine) -> pd.DataFrame:
    """Lê a tabela general_reports do banco, trazendo apenas as colunas necessárias.

    Ordena do serviço mais recente para o mais antigo para garantir que,
    em caso de duplicidade no merge, o registro mais novo seja priorizado.

    Args:
        engine: Conexão SQLAlchemy com o banco de dados.

    Returns:
        DataFrame com as colunas necessárias para o enriquecimento.
    """
    query = text("""
        SELECT
            "UC / MD"       AS instalacao,
            "COD"           AS irregularidade,
            "DATA_EXECUCAO" AS data_exec_rep,
            "DATA BAIXADO"  AS data_baixa_rep,
            "EQUIPE"        AS equipe
        FROM general_reports
        ORDER BY "DATA_EXECUCAO" DESC NULLS LAST
    """)

    df = pd.read_sql(query, engine)

    df['instalacao']    = pd.to_numeric(df['instalacao'], errors='coerce')
    df['irregularidade'] = pd.to_numeric(df['irregularidade'], errors='coerce')
    df['data_exec_rep']  = pd.to_datetime(df['data_exec_rep'], errors='coerce')
    df['data_baixa_rep'] = pd.to_datetime(df['data_baixa_rep'], errors='coerce')

    # Remove linhas sem instalação (chave obrigatória)
    df = df[df['instalacao'].notna()].copy()

    print(f'✅ [EXTRACT DB] {len(df)} registros carregados da general_reports.')
    return df