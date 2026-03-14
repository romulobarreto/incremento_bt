"""Módulo responsável pelo enriquecimento da base de incremento com dados da general_reports.

Aplica 3 regras de cruzamento em cascata, por ordem de prioridade:
    Regra 1: INSTALACAO + IRREGULARIDADE + MÊS/ANO DATA_EXECUCAO
    Regra 2: INSTALACAO + IRREGULARIDADE + MÊS/ANO DATA_BAIXA
    Regra 3: INSTALACAO + MÊS/ANO DATA_BAIXA + CLASSIFICACAO_IRREG (grupo de códigos)

Linhas que não encontrarem correspondência em nenhuma regra são exportadas
para output/servicos_sem_cruzamento.xlsx e removidas da carga final.
"""

import os
import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Mapa de irregularidade → classificação (usado na Regra 3)
MAPA_CLASSIFICACAO = {
    109: 'C100', 115: 'C100', 129: 'C100', 154: 'REGU',
    164: 'C100', 165: 'C100', 168: 'C100', 171: 'C100',
    172: 'C100', 174: 'C100', 175: 'REGU', 176: 'C100',
    188: 'C100',
    201: 'C200', 202: 'C200', 203: 'C200', 204: 'C200',
    210: 'C200', 211: 'C200', 212: 'C200', 214: 'C200',
    215: 'C200', 221: 'C200',
    300: 'ACAO', 301: 'ACAO', 302: 'ACAO', 303: 'ACAO',
    305: 'ACAO', 306: 'ACAO', 309: 'ACAO', 312: 'ACAO',
    314: 'ACAO', 315: 'ACAO',
    307: 'C300', 308: 'C300', 310: 'C300', 311: 'C300',
}


def _preparar_reports(df_rep: pd.DataFrame) -> pd.DataFrame:
    """Adiciona colunas de período (Mês/Ano) e classificação na general_reports."""
    df = df_rep.copy()
    df['mes_ano_exec']  = df['data_exec_rep'].dt.to_period('M')
    df['mes_ano_baixa'] = df['data_baixa_rep'].dt.to_period('M')
    df['classificacao'] = df['irregularidade'].map(MAPA_CLASSIFICACAO)
    return df


def _merge_e_preenche(
    df_inc: pd.DataFrame,
    df_rep: pd.DataFrame,
    chaves_inc: list[str],
    chaves_rep: list[str],
    mascara_pendentes: pd.Series,
) -> pd.DataFrame:
    """Realiza o merge para as linhas pendentes e preenche EQUIPE, DATA_EXECUCAO e IRREGULARIDADE.

    Args:
        df_inc: DataFrame principal (incremento).
        df_rep: DataFrame de referência (general_reports preparado).
        chaves_inc: Colunas-chave do df_inc para o join.
        chaves_rep: Colunas-chave do df_rep para o join.
        mascara_pendentes: Boolean Series indicando quais linhas ainda precisam ser preenchidas.

    Returns:
        df_inc atualizado com os campos preenchidos onde houve match.
    """
    df_pendentes = df_inc[mascara_pendentes].copy()

    # Pega apenas a primeira ocorrência por chave (mais recente, pois df_rep já está ordenado)
    df_rep_dedup = df_rep.drop_duplicates(subset=chaves_rep, keep='first')

    # Colunas extras a trazer do df_rep (além das chaves)
    # Evita duplicidade: só inclui se não estiver já nas chaves
    extras = [c for c in ['equipe', 'data_exec_rep', 'irregularidade'] if c not in chaves_rep]
    colunas_rep = chaves_rep + extras

    merged = df_pendentes.merge(
        df_rep_dedup[colunas_rep],
        left_on=chaves_inc,
        right_on=chaves_rep,
        how='left',
        suffixes=('', '_rep'),
    )
    merged.index = df_pendentes.index

    # Preenche EQUIPE
    achou = merged['equipe'].notna()
    df_inc.loc[mascara_pendentes & achou, 'EQUIPE'] = merged.loc[achou, 'equipe'].values

    # Preenche DATA_EXECUCAO se estava vazia
    if 'data_exec_rep' in merged.columns:
        sem_exec = df_inc['DATA_EXECUCAO'].isna()
        df_inc.loc[mascara_pendentes & achou & sem_exec, 'DATA_EXECUCAO'] = (
            merged.loc[achou & sem_exec[mascara_pendentes], 'data_exec_rep'].values
        )

    # Preenche IRREGULARIDADE se estava zerada
    # Pode vir como 'irregularidade' (extra) ou já estar nas chaves (via join)
    irreg_col = 'irregularidade_rep' if 'irregularidade_rep' in merged.columns else 'irregularidade'
    if irreg_col in merged.columns:
        sem_irreg = df_inc['IRREGULARIDADE'] == 0
        df_inc.loc[mascara_pendentes & achou & sem_irreg, 'IRREGULARIDADE'] = (
            merged.loc[achou & sem_irreg[mascara_pendentes], irreg_col].values
        )

    return df_inc


def enriquecer_incremento(
    df_inc: pd.DataFrame,
    df_rep: pd.DataFrame,
) -> pd.DataFrame:
    """Enriquece o DataFrame de incremento com EQUIPE, DATA_EXECUCAO e IRREGULARIDADE.

    Aplica as 3 regras de cruzamento em cascata. Linhas sem correspondência
    são exportadas para output/servicos_sem_cruzamento.xlsx.

    Args:
        df_inc: DataFrame transformado do incremento (saída do transform_incremento).
        df_rep: DataFrame da general_reports (saída do extract_database).

    Returns:
        DataFrame enriquecido, sem as linhas que não tiveram correspondência.
    """
    df_inc = df_inc.copy()
    df_inc['EQUIPE'] = None

    df_rep = _preparar_reports(df_rep)

    # Chaves de período no incremento
    df_inc['mes_ano_exec']  = pd.to_datetime(df_inc['DATA_EXECUCAO'], errors='coerce').dt.to_period('M')
    df_inc['mes_ano_baixa'] = pd.to_datetime(df_inc['DATA_BAIXA'], errors='coerce').dt.to_period('M')

    # -------------------------------------------------------
    # REGRA 1: INSTALACAO + IRREGULARIDADE + MÊS/ANO EXECUÇÃO
    # -------------------------------------------------------
    pendentes = df_inc['EQUIPE'].isna() & df_inc['DATA_EXECUCAO'].notna() & (df_inc['IRREGULARIDADE'] != 0)
    logger.info('🔍 [MERGE R1] %d linhas tentando Regra 1...', pendentes.sum())

    df_inc = _merge_e_preenche(
        df_inc, df_rep,
        chaves_inc=['INSTALACAO', 'IRREGULARIDADE', 'mes_ano_exec'],
        chaves_rep=['instalacao', 'irregularidade', 'mes_ano_exec'],
        mascara_pendentes=pendentes,
    )
    achou_r1 = pendentes & df_inc['EQUIPE'].notna()
    logger.info('✅ [MERGE R1] %d linhas resolvidas.', achou_r1.sum())

    # -------------------------------------------------------
    # REGRA 2: INSTALACAO + IRREGULARIDADE + MÊS/ANO BAIXA
    # -------------------------------------------------------
    pendentes = df_inc['EQUIPE'].isna() & (df_inc['IRREGULARIDADE'] != 0) & df_inc['mes_ano_baixa'].notna()
    logger.info('🔍 [MERGE R2] %d linhas tentando Regra 2...', pendentes.sum())

    df_inc = _merge_e_preenche(
        df_inc, df_rep,
        chaves_inc=['INSTALACAO', 'IRREGULARIDADE', 'mes_ano_baixa'],
        chaves_rep=['instalacao', 'irregularidade', 'mes_ano_baixa'],
        mascara_pendentes=pendentes,
    )
    achou_r2 = pendentes & df_inc['EQUIPE'].notna()
    logger.info('✅ [MERGE R2] %d linhas resolvidas.', achou_r2.sum())

    # -------------------------------------------------------
    # REGRA 3: INSTALACAO + MÊS/ANO BAIXA + CLASSIFICACAO_IRREG
    # -------------------------------------------------------
    pendentes = df_inc['EQUIPE'].isna() & df_inc['mes_ano_baixa'].notna()
    logger.info('🔍 [MERGE R3] %d linhas tentando Regra 3...', pendentes.sum())

    df_inc = _merge_e_preenche(
        df_inc, df_rep,
        chaves_inc=['INSTALACAO', 'mes_ano_baixa', 'CLASSIFICACAO_IRREG'],
        chaves_rep=['instalacao', 'mes_ano_baixa', 'classificacao'],
        mascara_pendentes=pendentes,
    )
    achou_r3 = pendentes & df_inc['EQUIPE'].notna()
    logger.info('✅ [MERGE R3] %d linhas resolvidas.', achou_r3.sum())

    # -------------------------------------------------------
    # SEPARAÇÃO: linhas com e sem correspondência
    # -------------------------------------------------------
    df_final  = df_inc[df_inc['EQUIPE'].notna()].copy()
    df_falhas = df_inc[df_inc['EQUIPE'].isna()].copy()

    if not df_falhas.empty:
        os.makedirs('output', exist_ok=True)
        caminho_falhas = 'output/servicos_sem_cruzamento.xlsx'
        df_falhas.drop(columns=['mes_ano_exec', 'mes_ano_baixa'], errors='ignore').to_excel(
            caminho_falhas, index=False
        )
        logger.warning(
            '⚠️ [MERGE] %d linhas sem correspondência exportadas para %s',
            len(df_falhas), caminho_falhas,
        )
    else:
        logger.info('🎉 [MERGE] Todas as linhas foram resolvidas com sucesso!')

    # Remove colunas auxiliares antes de retornar
    df_final = df_final.drop(columns=['mes_ano_exec', 'mes_ano_baixa'], errors='ignore')

    logger.info('✅ [MERGE] %d linhas prontas para carga.', len(df_final))

    # Reordena colunas colocando EQUIPE antes de MES_01
    colunas_ordenadas = [
        'GRUPO', 'PROJETO', 'CMPT', 'NOTA', 'INSTALACAO',
        'DATA_EXECUCAO', 'DATA_BAIXA', 'CLASSIFICACAO_IRREG', 'IRREGULARIDADE',
        'EQUIPE',
        'MES_01', 'MES_02', 'MES_03', 'MES_04', 'MES_05', 'MES_06',
        'MES_07', 'MES_08', 'MES_09', 'MES_10', 'MES_11', 'MES_12',
        'INC_TOTAL'
    ]
    df_final = df_final[colunas_ordenadas]
    return df_final