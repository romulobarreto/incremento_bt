"""Carregamento snapshot: TRUNCATE/DELETE + INSERT (sem DROP)."""

from __future__ import annotations
from typing import Optional

import pandas as pd
from sqlalchemy import Boolean, Date, Engine, String, create_engine, inspect, text

def criar_engine_supabase(db_uri: str) -> Engine:
    return create_engine(db_uri, pool_pre_ping=True)

def _tabela_existe(engine: Engine, nome_tabela: str, schema: Optional[str]) -> bool:
    insp = inspect(engine)
    return insp.has_table(nome_tabela, schema=schema)

def _truncate_compat(engine: Engine, tabela_qualificada: str) -> str:
    dialect = engine.dialect.name.lower()
    if dialect == "sqlite":
        return f"DELETE FROM {tabela_qualificada}"
    return f"TRUNCATE TABLE {tabela_qualificada}"

def carregar_incremento(
    df: pd.DataFrame,
    engine: Engine,
    nome_tabela: str = "increment_bt",
    schema: Optional[str] = None,
    replace_strategy: str = "truncate-insert",
    chunksize: int = 500,
) -> None:
    """Carga snapshot diária preservando dependências (views etc.)."""
    tabela = f"{schema}.{nome_tabela}" if schema else nome_tabela

    tipos_sql = {
        "DATA_EXECUCAO": Date(),
        "DATA_BAIXA": Date(),
        "MATCH_OK": Boolean(),
        "REGRA_ORIGEM": String(length=16),
        "MOTIVO_PENDENCIA": String(length=32),  # << novo
    }

    with engine.begin() as conn:
        if replace_strategy == "truncate-insert":
            if _tabela_existe(engine, nome_tabela, schema):
                conn.execute(text(_truncate_compat(engine, tabela)))

            df.to_sql(
                name=nome_tabela,
                con=conn,
                if_exists="append",
                index=False,
                schema=schema,
                method="multi",
                chunksize=chunksize,
                dtype=tipos_sql,
            )
        elif replace_strategy == "append":
            df.to_sql(
                name=nome_tabela,
                con=conn,
                if_exists="append",
                index=False,
                schema=schema,
                method="multi",
                chunksize=chunksize,
                dtype=tipos_sql,
            )
        else:
            raise ValueError("replace_strategy deve ser 'truncate-insert' ou 'append'.")