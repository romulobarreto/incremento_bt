"""Módulo responsável pelo enriquecimento da base de incremento com dados da general_reports.

Aplica 5 regras de cruzamento em cascata, por ordem de prioridade:
    Regra 1: INSTALACAO + IRREGULARIDADE + MÊS/ANO DATA_EXECUCAO
    Regra 2: INSTALACAO + IRREGULARIDADE + MÊS/ANO DATA_BAIXA
    Regra 3: INSTALACAO + MÊS/ANO DATA_BAIXA + CLASSIFICACAO_IRREG (grupo de códigos)
    Regra 4: INSTALACAO + IRREGULARIDADE + MÊS/ANO EXEC (para registros SEM data_baixa_rep),
             com fallback para MÊS/ANO BAIXA - 1 mês
    Regra 5: INSTALACAO + CLASSIFICACAO_IRREG + MÊS/ANO EXEC (para registros SEM data_baixa_rep),
             com fallback para MÊS/ANO BAIXA - 1 mês

Regras 4 e 5 cobrem casos onde general_reports não possui data_baixa_rep (bug do SIGOS).

Linhas que não encontrarem correspondência em nenhuma regra são exportadas
para output/servicos_sem_cruzamento.xlsx e removidas da carga final.
"""

import os
import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Mapa de irregularidade → classificação (usado nas Regras 3 e 5)
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

    # Garante datetime
    df["data_exec_rep"] = pd.to_datetime(df["data_exec_rep"], errors="coerce")
    df["data_baixa_rep"] = pd.to_datetime(df["data_baixa_rep"], errors="coerce")

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
    irreg_col = 'irregularidade_rep' if 'irregularidade_rep' in merged.columns else 'irregularidade'
    if irreg_col in merged.columns:
        sem_irreg = df_inc['IRREGULARIDADE'] == 0
        df_inc.loc[mascara_pendentes & achou & sem_irreg, 'IRREGULARIDADE'] = (
            merged.loc[achou & sem_irreg[mascara_pendentes], irreg_col].values
        )

    return df_inc


def _aplicar_regra_exec_mes_ano(
    df_inc: pd.DataFrame,
    df_rep_sem_baixa: pd.DataFrame,
    chaves_inc: list[str],
    chaves_rep: list[str],
    usar_mes_anterior: bool,
    mascara_base: pd.Series,
) -> pd.DataFrame:
    """
    Função auxiliar para Regras 4 e 5.

    Faz merge usando:
      - no incremento: mes_ano_baixa ou (mes_ano_baixa - 1 mês)
      - no reports: mes_ano_exec

    Args:
        df_inc: incremento.
        df_rep_sem_baixa: general_reports filtrado apenas com data_baixa_rep nula.
        chaves_inc: chaves no incremento (sem período).
        chaves_rep: chaves no reports    (sem período).
        usar_mes_anterior: se True, usa mes_ano_baixa - 1 mês.
        mascara_base: linhas pendentes a considerar.

    Returns:
        df_inc atualizado.
    """
    df_inc_local = df_inc.copy()

    # Só faz sentido onde temos DATA_BAIXA
    pend = mascara_base & df_inc_local['DATA_BAIXA'].notna()
    if not pend.any():
        return df_inc_local

    # Períodos no incremento
    data_baixa = pd.to_datetime(df_inc_local.loc[pend, 'DATA_BAIXA'], errors='coerce')
    periodo = data_baixa.dt.to_period('M')
    if usar_mes_anterior:
        periodo = (periodo - 1)  # período anterior

    df_inc_local.loc[pend, 'mes_ano_exec_proxy'] = periodo

    # Período no reports: já temos mes_ano_exec
    df_rep_local = df_rep_sem_baixa.copy()

    chaves_inc_full = chaves_inc + ['mes_ano_exec_proxy']
    chaves_rep_full = chaves_rep + ['mes_ano_exec']

    df_inc_local = _merge_e_preenche(
        df_inc_local,
        df_rep_local,
        chaves_inc=chaves_inc_full,
        chaves_rep=chaves_rep_full,
        mascara_pendentes=pend,
    )

    # Limpa coluna auxiliar
    df_inc_local.drop(columns=['mes_ano_exec_proxy'], inplace=True, errors='ignore')
    return df_inc_local


def enriquecer_incremento(
    df_inc: pd.DataFrame,
    df_rep: pd.DataFrame,
) -> pd.DataFrame:
    """Enriquece o DataFrame de incremento com EQUIPE, DATA_EXECUCAO e IRREGULARIDADE.

    Aplica as 5 regras de cruzamento em cascata. Linhas sem correspondência
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

    # ------------------------------------------------------------------
    # PREPARO PARA REGRAS 4 e 5: apenas registros do reports sem data_baixa_rep
    # ------------------------------------------------------------------
    df_rep_sem_baixa = df_rep[df_rep['data_baixa_rep'].isna()].copy()
    logger.info('ℹ️ [MERGE] %d registros no general_reports sem data_baixa_rep para Regras 4 e 5.',
                len(df_rep_sem_baixa))

    # -------------------------------------------------------
    # REGRA 4: INSTALACAO + IRREGULARIDADE + MÊS/ANO EXEC
    #         (sem data_baixa_rep) + fallback mês anterior
    # -------------------------------------------------------
    pendentes = df_inc['EQUIPE'].isna() & (df_inc['IRREGULARIDADE'] != 0)
    logger.info('🔍 [MERGE R4] %d linhas tentando Regra 4 (mes_ano_exec / mes_ano_exec-1)...', pendentes.sum())

    # Primeiro tenta com mes_ano_baixa == mes_ano_exec
    df_inc = _aplicar_regra_exec_mes_ano(
        df_inc,
        df_rep_sem_baixa,
        chaves_inc=['INSTALACAO', 'IRREGULARIDADE'],
        chaves_rep=['instalacao', 'irregularidade'],
        usar_mes_anterior=False,
        mascara_base=pendentes,
    )
    achou_r4_1 = pendentes & df_inc['EQUIPE'].notna()
    logger.info('✅ [MERGE R4] %d linhas resolvidas na primeira passada (mes_ano igual).', achou_r4_1.sum())

    # Depois tenta com mês anterior para quem ainda ficou pendente
    pendentes_r4_2 = df_inc['EQUIPE'].isna() & (df_inc['IRREGULARIDADE'] != 0)
    df_inc = _aplicar_regra_exec_mes_ano(
        df_inc,
        df_rep_sem_baixa,
        chaves_inc=['INSTALACAO', 'IRREGULARIDADE'],
        chaves_rep=['instalacao', 'irregularidade'],
        usar_mes_anterior=True,
        mascara_base=pendentes_r4_2,
    )
    achou_r4_2 = pendentes_r4_2 & df_inc['EQUIPE'].notna()
    logger.info('✅ [MERGE R4] %d linhas resolvidas na segunda passada (mes_ano - 1).', achou_r4_2.sum())

    # -------------------------------------------------------
    # REGRA 5: INSTALACAO + CLASSIFICACAO_IRREG + MÊS/ANO EXEC
    #         (sem data_baixa_rep) + fallback mês anterior
    # -------------------------------------------------------
    pendentes = df_inc['EQUIPE'].isna()
    logger.info('🔍 [MERGE R5] %d linhas tentando Regra 5 (mes_ano_exec / mes_ano_exec-1)...', pendentes.sum())

    # Primeiro tenta com mes_ano_baixa == mes_ano_exec
    df_inc = _aplicar_regra_exec_mes_ano(
        df_inc,
        df_rep_sem_baixa,
        chaves_inc=['INSTALACAO', 'CLASSIFICACAO_IRREG'],
        chaves_rep=['instalacao', 'classificacao'],
        usar_mes_anterior=False,
        mascara_base=pendentes,
    )
    achou_r5_1 = pendentes & df_inc['EQUIPE'].notna()
    logger.info('✅ [MERGE R5] %d linhas resolvidas na primeira passada (mes_ano igual).', achou_r5_1.sum())

    # Depois tenta com mês anterior para quem ainda ficou pendente
    pendentes_r5_2 = df_inc['EQUIPE'].isna()
    df_inc = _aplicar_regra_exec_mes_ano(
        df_inc,
        df_rep_sem_baixa,
        chaves_inc=['INSTALACAO', 'CLASSIFICACAO_IRREG'],
        chaves_rep=['instalacao', 'classificacao'],
        usar_mes_anterior=True,
        mascara_base=pendentes_r5_2,
    )
    achou_r5_2 = pendentes_r5_2 & df_inc['EQUIPE'].notna()
    logger.info('✅ [MERGE R5] %d linhas resolvidas na segunda passada (mes_ano - 1).', achou_r5_2.sum())

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