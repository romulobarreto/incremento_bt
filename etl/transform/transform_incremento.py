"""Módulo responsável pela transformação dos dados de incremento."""

import pandas as pd
import numpy as np

COLUNAS_NECESSARIAS = [
    'GRUPO', 'PROJETO', 'CMPT', 'NOTA', 'INSTALACAO',
    'DATA_EXECUCAO', 'DATA_BAIXA', 'CLASSIFICACAO_IRREG', 'IRREGULARIDADE',
    'MES_01', 'MES_02', 'MES_03', 'MES_04', 'MES_05', 'MES_06',
    'MES_07', 'MES_08', 'MES_09', 'MES_10', 'MES_11', 'MES_12',
    'INC_TOTAL'
]

def transformar_incremento(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma os dados tratando nulos e tipos para o Supabase."""

    # 1. Filtra colunas
    df = df[COLUNAS_NECESSARIAS].copy()

    # 2. Datas: converte e mantém como datetime64 (pandas/SQLAlchemy envia como DATE)
    # Linhas vazias viram NaT, que o SQLAlchemy converte para NULL no banco
    df['DATA_EXECUCAO'] = pd.to_datetime(df['DATA_EXECUCAO'], errors='coerce').dt.normalize()
    df['DATA_BAIXA'] = pd.to_datetime(df['DATA_BAIXA'], errors='coerce').dt.normalize()

    # 3. Inteiros: preenche vazio com 0
    cols_inteiras = [
        'INSTALACAO', 'CMPT', 'IRREGULARIDADE', 'INC_TOTAL',
        'MES_01', 'MES_02', 'MES_03', 'MES_04', 'MES_05', 'MES_06',
        'MES_07', 'MES_08', 'MES_09', 'MES_10', 'MES_11', 'MES_12'
    ]
    for col in cols_inteiras:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    # 4. Textos: garante string e limpa 'nan'
    cols_texto = ['GRUPO', 'PROJETO', 'NOTA', 'CLASSIFICACAO_IRREG']
    for col in cols_texto:
        df[col] = df[col].astype(str).replace(['nan', 'None', 'NaN'], '')

    print(f'✅ [TRANSFORM] {len(df)} linhas tratadas.')

    # 5. Corrige CLASSIFICACAO_IRREG com base no código de IRREGULARIDADE
    mapa_classificacao = {
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

    df['CLASSIFICACAO_IRREG'] = df['IRREGULARIDADE'].map(mapa_classificacao).fillna(df['CLASSIFICACAO_IRREG'])
    return df