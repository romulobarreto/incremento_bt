"""Módulo responsável pelo carregamento dos dados no banco de dados.

Implementa uma estratégia de substituição segura do conteúdo da tabela
sem dropar a estrutura, preservando dependências como views (em Postgres).

Estratégias:
- 'truncate-insert': TRUNCATE (ou equivalente) + INSERT em transação.
- 'append': apenas adiciona as linhas ao final da tabela.
"""

from typing import Optional

import pandas as pd
from sqlalchemy import Date, Engine, create_engine, inspect, text


def criar_engine_supabase(db_uri: str) -> Engine:
    """Cria um engine SQLAlchemy para conexão com o banco de dados.

    Args:
        db_uri: URI de conexão no formato
            postgresql://usuario:senha@host:porta/database

    Returns:
        Engine configurado para operações no banco.
    """
    return create_engine(db_uri, pool_pre_ping=True)


def _tabela_existe(engine: Engine, nome_tabela: str, schema: Optional[str]) -> bool:
    """Verifica se a tabela existe no banco, considerando o schema."""
    insp = inspect(engine)
    # SQLAlchemy 2.x: Inspector.has_table(table_name, schema=None)
    return insp.has_table(nome_tabela, schema=schema)


def _truncate_compat(engine: Engine, tabela_qualificada: str) -> str:
    """Retorna o SQL apropriado para limpar a tabela por dialeto.

    Em Postgres: TRUNCATE TABLE.
    Em SQLite: DELETE FROM (não há TRUNCATE).
    """
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
    """Carrega dados no banco usando estratégia segura para snapshots.

    Para snapshots completos, utilize 'truncate-insert' para evitar DROP
    da tabela (o que quebraria views dependentes). A operação ocorre
    dentro de uma transação, garantindo atomicidade.

    Args:
        df: DataFrame pronto para carga.
        engine: Conexão SQLAlchemy com o banco alvo.
        nome_tabela: Nome da tabela de destino.
        schema: Schema do banco (ex.: 'public'). Use None para padrão.
        replace_strategy: Estratégia de carga. Valores:
            - 'truncate-insert': limpa e insere (recomendado para snapshot).
            - 'append': adiciona linhas.
        chunksize: Tamanho dos lotes no INSERT (otimiza performance).

    Raises:
        ValueError: Quando replace_strategy não é suportado.
    """
    tabela = f"{schema}.{nome_tabela}" if schema else nome_tabela

    tipos_sql = {
        'DATA_EXECUCAO': Date(),
        'DATA_BAIXA': Date(),
    }

    with engine.begin() as conn:
        if replace_strategy == "truncate-insert":
            # Limpa apenas se a tabela já existir
            if _tabela_existe(engine, nome_tabela, schema):
                sql_limpeza = _truncate_compat(engine, tabela)
                conn.execute(text(sql_limpeza))

            # Insere tudo (cria a tabela se ainda não existir)
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
            raise ValueError(
                "replace_strategy deve ser 'truncate-insert' ou 'append'."
            )