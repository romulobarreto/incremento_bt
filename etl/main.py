"""Script principal do pipeline ETL de incremento."""

import logging
import os
from datetime import datetime

from dotenv import load_dotenv
from tqdm import tqdm

from etl.extract.extract_excel import ler_excel
from etl.extract.extract_database import ler_general_reports
from etl.load.load_incremento import carregar_incremento, criar_engine_supabase
from etl.transform.transform_incremento import transformar_incremento
from etl.transform.merge_reports import enriquecer_incremento


def configurar_logs() -> logging.Logger:
    """Configura o logger para gravar em arquivo e no console.

    Cria a pasta logs/ automaticamente se não existir.

    Returns:
        Logger configurado.
    """
    os.makedirs('logs', exist_ok=True)

    nome_arquivo = datetime.now().strftime('logs/etl_%Y-%m-%d_%H-%M-%S.log')

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(nome_arquivo, encoding='utf-8'),
            logging.StreamHandler(),
        ],
    )

    return logging.getLogger(__name__)


def main() -> None:
    """Executa o pipeline ETL completo: Extract → Transform → Merge → Load."""
    load_dotenv()
    logger = configurar_logs()

    logger.info('🚀 Iniciando pipeline ETL de incremento...')

    etapas = ['Extract Excel', 'Extract Database', 'Transform', 'Merge', 'Load']

    with tqdm(total=len(etapas), desc='Pipeline ETL', unit='etapa') as barra:

        # ==================
        # EXTRACT EXCEL
        # ==================
        try:
            logger.info('📂 [EXTRACT] Lendo arquivo Excel da pasta input...')
            df_bruto = ler_excel('input')
            logger.info('✅ [EXTRACT] %d linhas extraídas do Excel.', len(df_bruto))
            barra.update(1)
        except FileNotFoundError as e:
            logger.error('❌ [EXTRACT] Arquivo não encontrado: %s', e)
            raise

        # ==================
        # EXTRACT DATABASE
        # ==================
        try:
            logger.info('🔌 [EXTRACT DB] Conectando ao Supabase...')
            db_uri = os.getenv('SUPABASE_DB_URI')

            if not db_uri:
                raise ValueError('Variável SUPABASE_DB_URI não encontrada no .env')

            engine = criar_engine_supabase(db_uri)
            df_reports = ler_general_reports(engine)
            logger.info('✅ [EXTRACT DB] %d registros carregados da general_reports.', len(df_reports))
            barra.update(1)
        except Exception as e:
            logger.error('❌ [EXTRACT DB] Erro ao buscar general_reports: %s', e)
            raise

        # ==================
        # TRANSFORM
        # ==================
        try:
            logger.info('🔄 [TRANSFORM] Aplicando transformações...')
            df_tratado = transformar_incremento(df_bruto)
            logger.info('✅ [TRANSFORM] %d linhas transformadas.', len(df_tratado))
            barra.update(1)
        except KeyError as e:
            logger.error('❌ [TRANSFORM] Coluna não encontrada: %s', e)
            raise

        # ==================
        # MERGE
        # ==================
        try:
            logger.info('🧠 [MERGE] Cruzando incremento com general_reports...')
            df_enriquecido = enriquecer_incremento(df_tratado, df_reports)
            logger.info('✅ [MERGE] %d linhas prontas para carga.', len(df_enriquecido))
            barra.update(1)
        except Exception as e:
            logger.error('❌ [MERGE] Erro no enriquecimento: %s', e)
            raise

        # ==================
        # LOAD
        # ==================
        try:
            logger.info('📤 [LOAD] Carregando dados na tabela increment_bt...')

            carregar_incremento(
                df=df_enriquecido,
                engine=engine,
                nome_tabela='increment_bt',
                schema=None,
                replace_strategy='truncate-insert',
                chunksize=500,
            )

            logger.info('✅ [LOAD] %d linhas carregadas na tabela increment_bt.', len(df_enriquecido))
            barra.update(1)
        except Exception as e:
            logger.error('❌ [LOAD] Erro ao carregar dados: %s', e)
            raise

    logger.info('🏁 Pipeline ETL finalizado com sucesso!')


if __name__ == '__main__':
    main()