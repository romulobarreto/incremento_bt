"""Módulo responsável pela extração de arquivos Excel da pasta input."""

import os

import pandas as pd


def ler_excel(pasta_input: str = 'input') -> pd.DataFrame:
    """Lê o primeiro arquivo Excel encontrado na pasta input.

    Args:
        pasta_input: Caminho da pasta onde o arquivo Excel está localizado.

    Returns:
        DataFrame com os dados brutos do arquivo Excel.

    Raises:
        FileNotFoundError: Se nenhum arquivo .xlsx for encontrado na pasta.
    """
    arquivos = [f for f in os.listdir(pasta_input) if f.endswith('.xlsx')]

    if not arquivos:
        raise FileNotFoundError(
            f'Nenhum arquivo .xlsx encontrado na pasta "{pasta_input}".'
        )

    caminho = os.path.join(pasta_input, arquivos[0])
    print(f'📂 Lendo arquivo: {arquivos[0]}')

    df = pd.read_excel(caminho, dtype=str)

    print(f'✅ {len(df)} linhas extraídas com sucesso.')
    return df
