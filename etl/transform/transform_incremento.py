"""Módulo responsável pela transformação dos dados de incremento."""

import pandas as pd

COLUNAS_NECESSARIAS = [
    'INSTALACAO',
    'DATA_NOTA',
    'DATA_BAIXA',
    'IRREGULARIDADE',
    'INC_TOTAL',
]


def transformar_incremento(df: pd.DataFrame) -> pd.DataFrame:
    """Seleciona e transforma as colunas necessárias do DataFrame de incremento.

    Transformações aplicadas:
        - Seleção apenas das colunas necessárias.
        - INSTALACAO: convertida para inteiro.
        - DATA_NOTA: convertida para datetime.
        - DATA_BAIXA: convertida para datetime.
        - IRREGULARIDADE: nulos preenchidos com 0 e convertida para inteiro.
        - INC_TOTAL: convertida para inteiro.

    Args:
        df: DataFrame bruto extraído do arquivo Excel.

    Returns:
        DataFrame transformado com apenas as colunas necessárias e tipos corretos.

    Raises:
        KeyError: Se alguma coluna necessária não existir no DataFrame.
    """
    colunas_faltando = [c for c in COLUNAS_NECESSARIAS if c not in df.columns]
    if colunas_faltando:
        raise KeyError(
            f'Colunas não encontradas no DataFrame: {colunas_faltando}'
        )

    df = df[COLUNAS_NECESSARIAS].copy()

    df['INSTALACAO'] = df['INSTALACAO'].astype(int)
    df['DATA_NOTA'] = pd.to_datetime(df['DATA_NOTA']).dt.date
    df['DATA_BAIXA'] = pd.to_datetime(df['DATA_BAIXA']).dt.date
    df['IRREGULARIDADE'] = df['IRREGULARIDADE'].fillna(0).astype(int)
    df['INC_TOTAL'] = df['INC_TOTAL'].astype(int)

    print(f'✅ Transformação concluída. {len(df)} linhas prontas para carga.')
    return df
