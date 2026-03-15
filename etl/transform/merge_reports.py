"""Enriquecimento do incremento com cruzamento no general_reports (R1..R5 + R4-1/R5-1).

- Mantém regras originais R1..R5.
- Adiciona fallbacks R4-1 e R5-1 (mês-1), que comparam o MÊS/ANO da
  DATA_BAIXA do incremento com o MÊS/ANO de DATA_EXEC no reports mesmo
  quando o reports possui DATA_BAIXADO preenchida (sem a restrição de R4/R5).
- Exporta pendentes para output/servicos_sem_cruzamento.xlsx.
- Retorna TODAS as linhas (casadas e pendentes) com flags e telemetria:
  MATCH_OK, REGRA_ORIGEM e MOTIVO_PENDENCIA.
"""

from __future__ import annotations

import os
import logging
from typing import List, Set, Dict

import pandas as pd

logger = logging.getLogger(__name__)

# (opcional) mover para etl/shared/classificacao.py e importar de lá
MAPA_CLASSIFICACAO = {
    109: "C100", 115: "C100", 129: "C100", 154: "REGU",
    164: "C100", 165: "C100", 168: "C100", 171: "C100",
    172: "C100", 174: "C100", 175: "REGU", 176: "C100",
    188: "C100",
    201: "C200", 202: "C200", 203: "C200", 204: "C200",
    210: "C200", 211: "C200", 212: "C200", 214: "C200",
    215: "C200", 221: "C200",
    300: "ACAO", 301: "ACAO", 302: "ACAO", 303: "ACAO",
    305: "ACAO", 306: "ACAO", 309: "ACAO", 312: "ACAO",
    314: "ACAO", 315: "ACAO",
    307: "C300", 308: "C300", 310: "C300", 311: "C300",
}


def _preparar_reports(df_rep: pd.DataFrame) -> pd.DataFrame:
    """Garante datas/periodicidade, classificação e dtypes numéricos em Int64."""
    df = df_rep.copy()

    df["data_exec_rep"] = pd.to_datetime(df["data_exec_rep"], errors="coerce")
    df["data_baixa_rep"] = pd.to_datetime(df["data_baixa_rep"], errors="coerce")

    df["mes_ano_exec"] = df["data_exec_rep"].dt.to_period("M")
    df["mes_ano_baixa"] = df["data_baixa_rep"].dt.to_period("M")

    df["classificacao"] = df["irregularidade"].map(MAPA_CLASSIFICACAO)

    if "instalacao" in df.columns:
        df["instalacao"] = pd.to_numeric(df["instalacao"], errors="coerce").astype("Int64")
    if "irregularidade" in df.columns:
        df["irregularidade"] = pd.to_numeric(df["irregularidade"], errors="coerce").astype("Int64")

    return df


def _aplicar_match(
    df_inc: pd.DataFrame,
    merged: pd.DataFrame,
    mascara_pendentes: pd.Series,
    regra_id: str,
) -> pd.DataFrame:
    """Aplica preenchimentos e marca MATCH_OK/REGRA_ORIGEM com alinhamento seguro."""
    merged.index = df_inc[mascara_pendentes].index

    achou = merged["equipe"].notna()
    idx_achou = achou.index[achou]

    if len(idx_achou) > 0:
        # 1) Preenche EQUIPE
        df_inc.loc[idx_achou, "EQUIPE"] = merged.loc[idx_achou, "equipe"].values

        # 2) Preenche DATA_EXECUCAO se estava vazia
        if "data_exec_rep" in merged.columns:
            sem_exec = df_inc.loc[idx_achou, "DATA_EXECUCAO"].isna()
            idx = idx_achou[sem_exec]
            if len(idx) > 0:
                df_inc.loc[idx, "DATA_EXECUCAO"] = merged.loc[idx, "data_exec_rep"].values

        # 3) Preenche IRREGULARIDADE se estava 0
        if "irregularidade" in merged.columns:
            sem_irreg = df_inc.loc[idx_achou, "IRREGULARIDADE"].eq(0)
            idx = idx_achou[sem_irreg]
            if len(idx) > 0:
                df_inc.loc[idx, "IRREGULARIDADE"] = merged.loc[idx, "irregularidade"].values

    novos = mascara_pendentes & df_inc["EQUIPE"].notna() & ~df_inc["MATCH_OK"]
    df_inc.loc[novos, "MATCH_OK"] = True
    df_inc.loc[novos & df_inc["REGRA_ORIGEM"].isna(), "REGRA_ORIGEM"] = regra_id

    logger.info("✅ [%s] %d linhas resolvidas.", regra_id, int(novos.sum()))
    return df_inc


def _merge_generico(
    df_inc: pd.DataFrame,
    df_rep: pd.DataFrame,
    chaves_inc: List[str],
    chaves_rep: List[str],
    mascara_pendentes: pd.Series,
    regra_id: str,
) -> pd.DataFrame:
    df_pend = df_inc[mascara_pendentes].copy()
    df_rep_dedup = df_rep.drop_duplicates(subset=chaves_rep, keep="first")

    extras = [c for c in ["equipe", "data_exec_rep", "irregularidade"] if c not in chaves_rep]
    colunas_rep = chaves_rep + extras

    merged = df_pend.merge(
        df_rep_dedup[colunas_rep],
        left_on=chaves_inc,
        right_on=chaves_rep,
        how="left",
        suffixes=("", "_rep"),
    )
    return _aplicar_match(df_inc, merged, mascara_pendentes, regra_id)


def _aplicar_regra_exec_mes_ano(
    df_inc: pd.DataFrame,
    df_rep_base: pd.DataFrame,
    chaves_inc: List[str],
    chaves_rep: List[str],
    usar_mes_anterior: bool,
    mascara_base: pd.Series,
    regra_id: str,
) -> pd.DataFrame:
    """
    Usa MÊS/ANO BAIXA do incremento como proxy do MÊS/ANO EXEC no reports.
    Se usar_mes_anterior=True, usa (P_BAIXA - 1 mês).
    """
    df_local = df_inc.copy()

    pend = mascara_base & df_local["DATA_BAIXA"].notna()
    if not pend.any():
        logger.info("ℹ️ [%s] Nenhuma linha aplicável.", regra_id)
        return df_local

    data_baixa = pd.to_datetime(df_local.loc[pend, "DATA_BAIXA"], errors="coerce")
    periodo = data_baixa.dt.to_period("M")
    if usar_mes_anterior:
        periodo = periodo - 1

    df_local.loc[pend, "mes_ano_exec_proxy"] = periodo

    ch_inc_full = chaves_inc + ["mes_ano_exec_proxy"]
    ch_rep_full = chaves_rep + ["mes_ano_exec"]

    df_rep_dedup = df_rep_base.drop_duplicates(subset=ch_rep_full, keep="first")

    extras = [c for c in ["equipe", "data_exec_rep", "irregularidade"] if c not in ch_rep_full]
    colunas_rep = ch_rep_full + extras

    df_pend = df_local[pend].copy()
    merged = df_pend.merge(
        df_rep_dedup[colunas_rep],
        left_on=ch_inc_full,
        right_on=ch_rep_full,
        how="left",
        suffixes=("", "_rep"),
    )

    df_local = _aplicar_match(df_local, merged, pend, regra_id)
    df_local.drop(columns=["mes_ano_exec_proxy"], errors="ignore", inplace=True)
    return df_local


def _telemetria_pendentes(df: pd.DataFrame, df_rep: pd.DataFrame) -> pd.DataFrame:
    """
    Define MOTIVO_PENDENCIA para linhas sem match.

    - sem_uc_no_reports: UC não existe no reports
    - mismatch_mes: UC existe e também existe o mesmo COD em algum momento, mas não casou por mês
    - mismatch_cod: UC existe e existe CLASSIF compatível, mas COD não bateu
    - outros: nenhum dos casos acima
    """
    df_out = df.copy()
    pend = ~df_out["MATCH_OK"]
    if not pend.any():
        df_out["MOTIVO_PENDENCIA"] = None
        return df_out

    # Índices rápidos
    set_inst: Set[int] = set(df_rep["instalacao"].dropna().astype("Int64").dropna().tolist())

    codes_by_inst: Dict[int, Set[int]] = (
        df_rep.dropna(subset=["instalacao"])[["instalacao", "irregularidade"]]
        .dropna()
        .drop_duplicates()
        .groupby("instalacao")["irregularidade"]
        .apply(set)
        .to_dict()
    )

    class_by_inst: Dict[int, Set[str]] = (
        df_rep.dropna(subset=["instalacao"])[["instalacao", "classificacao"]]
        .dropna()
        .drop_duplicates()
        .groupby("instalacao")["classificacao"]
        .apply(set)
        .to_dict()
    )

    motivos = []
    for i, row in df_out.loc[pend].iterrows():
        uc = row["INSTALACAO"]
        cod = row["IRREGULARIDADE"]
        cls = row["CLASSIFICACAO_IRREG"]
        if pd.isna(uc) or uc not in set_inst:
            motivos.append((i, "sem_uc_no_reports"))
        else:
            codes = codes_by_inst.get(int(uc), set())
            classes = class_by_inst.get(int(uc), set())
            if pd.notna(cod) and int(cod) in codes:
                motivos.append((i, "mismatch_mes"))
            elif isinstance(cls, str) and cls in classes:
                motivos.append((i, "mismatch_cod"))
            else:
                motivos.append((i, "outros"))

    df_out["MOTIVO_PENDENCIA"] = None
    if motivos:
        idx, val = zip(*motivos)
        df_out.loc[list(idx), "MOTIVO_PENDENCIA"] = list(val)
    return df_out


def enriquecer_incremento(df_inc: pd.DataFrame, df_rep: pd.DataFrame) -> pd.DataFrame:
    """Aplica R1..R5, depois R4-1 e R5-1, e devolve todas as linhas com flags+telemetria."""
    df = df_inc.copy()

    # Flags
    df["EQUIPE"] = None
    df["MATCH_OK"] = False
    df["REGRA_ORIGEM"] = pd.Series([None] * len(df), dtype="object")

    # Harmonização de dtypes
    for col in ["INSTALACAO", "IRREGULARIDADE"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Reports preparado
    df_rep = _preparar_reports(df_rep)

    # Períodos no incremento
    df["mes_ano_exec"] = pd.to_datetime(df["DATA_EXECUCAO"], errors="coerce").dt.to_period("M")
    df["mes_ano_baixa"] = pd.to_datetime(df["DATA_BAIXA"], errors="coerce").dt.to_period("M")

    # ---------------- R1 ----------------
    pend = ~df["MATCH_OK"] & df["DATA_EXECUCAO"].notna() & df["IRREGULARIDADE"].notna() & (df["IRREGULARIDADE"] != 0)
    logger.info("🔍 [R1] Tentando %d linhas...", int(pend.sum()))
    df = _merge_generico(
        df, df_rep,
        chaves_inc=["INSTALACAO", "IRREGULARIDADE", "mes_ano_exec"],
        chaves_rep=["instalacao", "irregularidade", "mes_ano_exec"],
        mascara_pendentes=pend,
        regra_id="R1",
    )

    # ---------------- R2 ----------------
    pend = ~df["MATCH_OK"] & df["IRREGULARIDADE"].notna() & (df["IRREGULARIDADE"] != 0) & df["mes_ano_baixa"].notna()
    logger.info("🔍 [R2] Tentando %d linhas...", int(pend.sum()))
    df = _merge_generico(
        df, df_rep,
        chaves_inc=["INSTALACAO", "IRREGULARIDADE", "mes_ano_baixa"],
        chaves_rep=["instalacao", "irregularidade", "mes_ano_baixa"],
        mascara_pendentes=pend,
        regra_id="R2",
    )

    # ---------------- R3 ----------------
    pend = ~df["MATCH_OK"] & df["mes_ano_baixa"].notna()
    logger.info("🔍 [R3] Tentando %d linhas...", int(pend.sum()))
    df = _merge_generico(
        df, df_rep,
        chaves_inc=["INSTALACAO", "mes_ano_baixa", "CLASSIFICACAO_IRREG"],
        chaves_rep=["instalacao", "mes_ano_baixa", "classificacao"],
        mascara_pendentes=pend,
        regra_id="R3",
    )

    # ---------------- R4/R5 originais (apenas sem baixa no reports) ----------------
    df_rep_sem_baixa = df_rep[df_rep["data_baixa_rep"].isna()].copy()
    logger.info("ℹ️ [R4/R5] %d registros no reports sem data_baixa_rep.", int(len(df_rep_sem_baixa)))

    # R4: por código - mês igual
    pend = ~df["MATCH_OK"] & df["IRREGULARIDADE"].notna() & (df["IRREGULARIDADE"] != 0)
    df = _aplicar_regra_exec_mes_ano(
        df, df_rep_sem_baixa,
        chaves_inc=["INSTALACAO", "IRREGULARIDADE"],
        chaves_rep=["instalacao", "irregularidade"],
        usar_mes_anterior=False,
        mascara_base=pend,
        regra_id="R4",
    )

    # R4: por código - mês anterior
    pend = ~df["MATCH_OK"] & df["IRREGULARIDADE"].notna() & (df["IRREGULARIDADE"] != 0)
    df = _aplicar_regra_exec_mes_ano(
        df, df_rep_sem_baixa,
        chaves_inc=["INSTALACAO", "IRREGULARIDADE"],
        chaves_rep=["instalacao", "irregularidade"],
        usar_mes_anterior=True,
        mascara_base=pend,
        regra_id="R4",
    )

    # R5: por classificação - mês igual
    pend = ~df["MATCH_OK"]
    df = _aplicar_regra_exec_mes_ano(
        df, df_rep_sem_baixa,
        chaves_inc=["INSTALACAO", "CLASSIFICACAO_IRREG"],
        chaves_rep=["instalacao", "classificacao"],
        usar_mes_anterior=False,
        mascara_base=pend,
        regra_id="R5",
    )

    # R5: por classificação - mês anterior
    pend = ~df["MATCH_OK"]
    df = _aplicar_regra_exec_mes_ano(
        df, df_rep_sem_baixa,
        chaves_inc=["INSTALACAO", "CLASSIFICACAO_IRREG"],
        chaves_rep=["instalacao", "classificacao"],
        usar_mes_anterior=True,
        mascara_base=pend,
        regra_id="R5",
    )

    # ---------------- Novos fallbacks solicitados: R4-1 e R5-1 (sem restrição de baixa no reports) ----------------
    # R4-1: UC + COD, P_EXEC = (P_BAIXA - 1)
    pend = ~df["MATCH_OK"] & df["IRREGULARIDADE"].notna() & (df["IRREGULARIDADE"] != 0)
    df = _aplicar_regra_exec_mes_ano(
        df, df_rep,
        chaves_inc=["INSTALACAO", "IRREGULARIDADE"],
        chaves_rep=["instalacao", "irregularidade"],
        usar_mes_anterior=True,
        mascara_base=pend,
        regra_id="R4-1",
    )

    # R5-1: UC + CLASSIF, P_EXEC = (P_BAIXA - 1)
    pend = ~df["MATCH_OK"]
    df = _aplicar_regra_exec_mes_ano(
        df, df_rep,
        chaves_inc=["INSTALACAO", "CLASSIFICACAO_IRREG"],
        chaves_rep=["instalacao", "classificacao"],
        usar_mes_anterior=True,
        mascara_base=pend,
        regra_id="R5-1",
    )

    # Marcar explicitamente quem ficou sem match
    df.loc[~df["MATCH_OK"] & df["REGRA_ORIGEM"].isna(), "REGRA_ORIGEM"] = "SEM_MATCH"

    # Telemetria de pendentes
    df = _telemetria_pendentes(df, df_rep)

    # Exportar pendentes
    df_pend = df[~df["MATCH_OK"]].copy()
    df_pend.drop(columns=["mes_ano_exec", "mes_ano_baixa"], errors="ignore", inplace=True)
    if not df_pend.empty:
        os.makedirs("output", exist_ok=True)
        caminho = "output/servicos_sem_cruzamento.xlsx"
        df_pend.to_excel(caminho, index=False)
        logger.warning("⚠️ [MERGE] %d pendentes exportados para %s", len(df_pend), caminho)
    else:
        logger.info("🎉 [MERGE] Todas as linhas foram resolvidas com sucesso!")

    # Limpeza final e ordenação (mantém apenas o que existir)
    df.drop(columns=["mes_ano_exec", "mes_ano_baixa"], errors="ignore", inplace=True)
    colunas = [
        "GRUPO","PROJETO","CMPT","NOTA","INSTALACAO",
        "DATA_EXECUCAO","DATA_BAIXA","CLASSIFICACAO_IRREG","IRREGULARIDADE",
        "EQUIPE","MATCH_OK","REGRA_ORIGEM","MOTIVO_PENDENCIA",
        "MES_01","MES_02","MES_03","MES_04","MES_05","MES_06",
        "MES_07","MES_08","MES_09","MES_10","MES_11","MES_12",
        "INC_TOTAL",
    ]
    colunas = [c for c in colunas if c in df.columns]
    df = df[colunas]

    logger.info("✅ [MERGE] %d linhas prontas para carga (inclui pendentes).", len(df))
    return df