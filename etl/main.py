"""Script principal do pipeline ETL de incremento (v2)."""

from __future__ import annotations
import logging, os
from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm

from etl.extract.extract_excel import ler_excel
from etl.extract.extract_database import ler_general_reports
from etl.load.load_incremento import carregar_incremento, criar_engine_supabase, _tabela_existe
from etl.transform.transform_incremento import transformar_incremento
from etl.transform.merge_reports import enriquecer_incremento
from etl.load.run_sql_file import run_sql_file

def configurar_logs() -> logging.Logger:
    os.makedirs("logs", exist_ok=True)
    nome_arquivo = datetime.now().strftime("logs/etl_%Y-%m-%d_%H-%M-%S.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.FileHandler(nome_arquivo, encoding="utf-8"),
                  logging.StreamHandler()],
    )
    return logging.getLogger(__name__)

def main() -> None:
    load_dotenv()
    logger = configurar_logs()

    logger.info("🚀 Iniciando pipeline ETL de incremento...")
    etapas = ["Extract Excel","Extract Database","Transform","Merge","Load"]

    with tqdm(total=len(etapas), desc="Pipeline ETL", unit="etapa") as barra:
        # EXTRACT EXCEL
        logger.info("📂 [EXTRACT] Lendo arquivo Excel da pasta input...")
        df_bruto = ler_excel("input")
        logger.info("✅ [EXTRACT] %d linhas extraídas do Excel.", len(df_bruto)); barra.update(1)

        # EXTRACT DATABASE
        logger.info("🔌 [EXTRACT DB] Conectando ao Supabase...")
        db_uri = os.getenv("SUPABASE_DB_URI")
        if not db_uri:
            raise ValueError("Variável SUPABASE_DB_URI não encontrada no .env")
        engine = criar_engine_supabase(db_uri)
        df_reports = ler_general_reports(engine)
        logger.info("✅ [EXTRACT DB] %d registros carregados da general_reports.", len(df_reports)); barra.update(1)

        # TRANSFORM
        logger.info("🔄 [TRANSFORM] Aplicando transformações...")
        df_tratado = transformar_incremento(df_bruto)
        logger.info("✅ [TRANSFORM] %d linhas transformadas.", len(df_tratado)); barra.update(1)

        # MERGE
        logger.info("🧠 [MERGE] Cruzando incremento com general_reports...")
        df_final = enriquecer_incremento(df_tratado, df_reports)
        casadas = int(df_final["MATCH_OK"].sum())
        pendentes = int((~df_final["MATCH_OK"]).sum())
        logger.info("✅ [MERGE] %d casadas | %d pendentes.", casadas, pendentes); barra.update(1)

        # verificar existência da tabela ANTES do LOAD <<<
        nome_tabela = "increment_bt"
        schema = None  # ajuste se usar schema
        tabela_ja_existia = _tabela_existe(engine, nome_tabela, schema)

        # LOAD
        logger.info("📤 [LOAD] Carregando dados na tabela %s...", nome_tabela)
        carregar_incremento(
            df=df_final, engine=engine, nome_tabela=nome_tabela,
            schema=schema, replace_strategy="truncate-insert", chunksize=500,
        )
        logger.info("✅ [LOAD] %d linhas carregadas na tabela %s.", len(df_final), nome_tabela); barra.update(1)

        # se a tabela NÃO existia antes, cria/verifica índices
        if not tabela_ja_existia:
            logger.info("🛠️ [POST-LOAD] Criando/verificando índices (primeira criação da tabela)...")
            # arquivo .sql (na raiz do projeto): sql/create_indexes_increment_bt.sql
            run_sql_file(engine, "sql/create_indexes_increment_bt.sql")
            logger.info("✅ [POST-LOAD] Índices aplicados com sucesso.")
        else:
            logger.info("ℹ️ [POST-LOAD] Tabela já existia; índices não foram reaplicados.")

    logger.info("🏁 Pipeline ETL finalizado com sucesso!")

if __name__ == "__main__":
    main()