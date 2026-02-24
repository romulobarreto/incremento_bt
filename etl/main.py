"""Script principal do pipeline ETL de incremento."""

import logging
import os
from datetime import datetime

from dotenv import load_dotenv
from tqdm import tqdm

from etl.extract.extract_excel import ler_excel
from etl.load.load_incremento import carregar_incremento, criar_engine_supabase
from etl.transform.transform_incremento import transformar_incremento


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
    """Executa o pipeline ETL completo: Extract → Transform → Load."""
    load_dotenv()
    logger = configurar_logs()

    logger.info('🚀 Iniciando pipeline ETL de incremento...')

    etapas = ['Extract', 'Transform', 'Load']

    with tqdm(total=len(etapas), desc='Pipeline ETL', unit='etapa') as barra:

        # ==================
        # EXTRACT
        # ==================
        try:
            logger.info('📂 [EXTRACT] Lendo arquivo Excel da pasta input...')
            df_bruto = ler_excel('input')
            logger.info(
                f'✅ [EXTRACT] {len(df_bruto)} linhas extraídas com sucesso.'
            )
            barra.update(1)
        except FileNotFoundError as e:
            logger.error(f'❌ [EXTRACT] Arquivo não encontrado: {e}')
            raise

        # ==================
        # TRANSFORM
        # ==================
        try:
            logger.info('🔄 [TRANSFORM] Aplicando transformações...')
            df_tratado = transformar_incremento(df_bruto)
            logger.info(
                f'✅ [TRANSFORM] {len(df_tratado)} linhas transformadas com sucesso.'
            )
            barra.update(1)
        except KeyError as e:
            logger.error(f'❌ [TRANSFORM] Coluna não encontrada: {e}')
            raise

        # ==================
        # LOAD
        # ==================
        try:
            logger.info("📤 [LOAD] Conectando ao Supabase e carregando dados...")
            db_uri = os.getenv("SUPABASE_DB_URI")

            if not db_uri:
                raise ValueError("Variável SUPABASE_DB_URI não encontrada no .env")

            engine = criar_engine_supabase(db_uri)

            carregar_incremento(
                df=df_tratado,
                engine=engine,
                nome_tabela="increment_bt",
                schema=None,  # ajuste para 'public' se necessário
                replace_strategy="truncate-insert",
                chunksize=5000,
            )

            logger.info(
                "✅ [LOAD] %s linhas carregadas na tabela increment_bt.",
                len(df_tratado),
            )
            barra.update(1)
        except Exception as e:
            logger.error("❌ [LOAD] Erro ao carregar dados: %s", e)
            raise

    logger.info('🏁 Pipeline ETL finalizado com sucesso!')


if __name__ == '__main__':
    main()
