"""Testes para o módulo de carga dos dados de incremento."""

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from etl.load.load_incremento import carregar_incremento, criar_engine_supabase


def _sqlite_mem_compartilhado():
    """Cria engine SQLite in-memory compartilhado entre conexões."""
    return create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


def test_criar_engine_supabase_retorna_engine():
    """Deve retornar um objeto Engine válido."""
    db_uri = "sqlite:///:memory:"
    engine = criar_engine_supabase(db_uri)
    assert engine is not None


def test_carregar_incremento_cria_tabela_e_insere_dados():
    """Deve criar a tabela e inserir os dados no banco."""
    engine = _sqlite_mem_compartilhado()

    df = pd.DataFrame(
        {
            "INSTALACAO": [11126302, 72288981],
            "DATA_NOTA": pd.to_datetime(
                ["2026-01-06 00:00:00", "2025-12-29 00:00:00"]
            ),
            "DATA_BAIXA": pd.to_datetime(
                ["2026-01-16 00:00:00", "2026-01-03 00:00:00"]
            ),
            "IRREGULARIDADE": [311, 175],
            "INC_TOTAL": [0, 56],
        }
    )

    nome_tabela = "incremento_diario_teste"

    carregar_incremento(
        df=df,
        engine=engine,
        nome_tabela=nome_tabela,
        schema=None,
        replace_strategy="truncate-insert",
        chunksize=2000,
    )

    with engine.connect() as conn:
        resultado = conn.execute(
            text(f"SELECT COUNT(*) FROM {nome_tabela}")
        ).scalar()

    assert resultado == len(df)


def test_carregar_incremento_append_funciona():
    """Deve adicionar linhas quando replace_strategy='append'."""
    engine = _sqlite_mem_compartilhado()

    df1 = pd.DataFrame(
        {
            "INSTALACAO": [1],
            "DATA_NOTA": pd.to_datetime(["2026-01-01"]),
            "DATA_BAIXA": pd.to_datetime(["2026-01-02"]),
            "IRREGULARIDADE": [10],
            "INC_TOTAL": [100],
        }
    )

    df2 = pd.DataFrame(
        {
            "INSTALACAO": [2],
            "DATA_NOTA": pd.to_datetime(["2026-01-03"]),
            "DATA_BAIXA": pd.to_datetime(["2026-01-04"]),
            "IRREGULARIDADE": [20],
            "INC_TOTAL": [200],
        }
    )

    nome_tabela = "incremento_diario_teste_append"

    # Primeiro snapshot para criar a tabela
    carregar_incremento(
        df=df1,
        engine=engine,
        nome_tabela=nome_tabela,
        schema=None,
        replace_strategy="truncate-insert",
        chunksize=1000,
    )

    # Agora append
    carregar_incremento(
        df=df2,
        engine=engine,
        nome_tabela=nome_tabela,
        schema=None,
        replace_strategy="append",
        chunksize=1000,
    )

    with engine.connect() as conn:
        resultado = conn.execute(
            text(f"SELECT COUNT(*) FROM {nome_tabela}")
        ).scalar()

    assert resultado == 2