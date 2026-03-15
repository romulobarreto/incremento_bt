"""Cria os indexes na tabela do incremento."""
from __future__ import annotations
from sqlalchemy import Engine, text

def run_sql_file(engine: Engine, path_sql: str) -> None:
    """Executa o conteúdo inteiro de um arquivo .sql dentro de uma transação."""
    with open(path_sql, "r", encoding="utf-8") as f:
        sql_script = f.read()
    with engine.begin() as conn:
        conn.execute(text(sql_script))
