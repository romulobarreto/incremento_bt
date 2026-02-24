"""Testes para o módulo de transformação dos dados de incremento."""

import pandas as pd
import pytest

from etl.transform.transform_incremento import transformar_incremento


@pytest.fixture
def df_bruto():
    """Retorna um DataFrame simulando os dados brutos do Excel."""
    return pd.DataFrame(
        {
            'INSTALACAO': ['0011126302', '0072288981', '0070354642'],
            'DATA_NOTA': [
                '2026-01-06 00:00:00',
                '2025-12-29 00:00:00',
                '2026-01-08 00:00:00',
            ],
            'DATA_BAIXA': [
                '2026-01-16 00:00:00',
                '2026-01-03 00:00:00',
                '2026-01-08 00:00:00',
            ],
            'IRREGULARIDADE': ['0311', None, '0115'],
            'INC_TOTAL': ['0', '56', '90'],
            'COLUNA_EXTRA': ['x', 'y', 'z'],
        }
    )


@pytest.fixture
def df_sem_coluna(df_bruto):
    """Retorna um DataFrame sem uma coluna necessária."""
    return df_bruto.drop(columns=['INC_TOTAL'])


def test_retorna_dataframe(df_bruto):
    """Deve retornar um DataFrame."""
    resultado = transformar_incremento(df_bruto)
    assert isinstance(resultado, pd.DataFrame)


def test_somente_colunas_necessarias(df_bruto):
    """Deve retornar apenas as 5 colunas necessárias."""
    resultado = transformar_incremento(df_bruto)
    assert list(resultado.columns) == [
        'INSTALACAO',
        'DATA_NOTA',
        'DATA_BAIXA',
        'IRREGULARIDADE',
        'INC_TOTAL',
    ]


def test_instalacao_como_inteiro(df_bruto):
    """INSTALACAO deve ser convertida para inteiro."""
    resultado = transformar_incremento(df_bruto)
    assert resultado['INSTALACAO'].dtype == 'int64'


def test_data_nota_como_date(df_bruto):
    """DATA_NOTA deve ser convertida para date sem horário."""
    resultado = transformar_incremento(df_bruto)
    assert resultado['DATA_NOTA'].dtype == 'object'
    assert hasattr(resultado['DATA_NOTA'].iloc[0], 'year')


def test_data_baixa_como_date(df_bruto):
    """DATA_BAIXA deve ser convertida para date sem horário."""
    resultado = transformar_incremento(df_bruto)
    assert resultado['DATA_BAIXA'].dtype == 'object'
    assert hasattr(resultado['DATA_BAIXA'].iloc[0], 'year')


def test_irregularidade_nulo_vira_zero(df_bruto):
    """IRREGULARIDADE nula deve ser preenchida com 0."""
    resultado = transformar_incremento(df_bruto)
    assert resultado['IRREGULARIDADE'].isnull().sum() == 0
    assert resultado['IRREGULARIDADE'].dtype == 'int64'


def test_inc_total_como_inteiro(df_bruto):
    """INC_TOTAL deve ser convertida para inteiro."""
    resultado = transformar_incremento(df_bruto)
    assert resultado['INC_TOTAL'].dtype == 'int64'


def test_coluna_faltando_lanca_erro(df_sem_coluna):
    """Deve lançar KeyError se uma coluna necessária estiver faltando."""
    with pytest.raises(KeyError):
        transformar_incremento(df_sem_coluna)
