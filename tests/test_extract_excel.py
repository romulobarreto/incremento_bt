"""Testes para o módulo de extração de arquivos Excel."""

import os

import pandas as pd
import pytest

from etl.extract.extract_excel import ler_excel


@pytest.fixture
def pasta_com_excel(tmp_path):
    """Cria uma pasta temporária com um arquivo Excel de teste."""
    df = pd.DataFrame(
        {
            'INSTALACAO': ['123', '456'],
            'STATUS': ['ATIVO', 'INATIVO'],
            'MUNICIPIO': ['PORTO ALEGRE', 'CANOAS'],
        }
    )
    caminho_arquivo = tmp_path / 'teste.xlsx'
    df.to_excel(caminho_arquivo, index=False)
    return str(tmp_path)


@pytest.fixture
def pasta_sem_excel(tmp_path):
    """Cria uma pasta temporária sem nenhum arquivo Excel."""
    return str(tmp_path)


def test_ler_excel_retorna_dataframe(pasta_com_excel):
    """Deve retornar um DataFrame quando o arquivo Excel existe."""
    df = ler_excel(pasta_com_excel)
    assert isinstance(df, pd.DataFrame)


def test_ler_excel_tem_dados(pasta_com_excel):
    """Deve retornar um DataFrame com pelo menos uma linha."""
    df = ler_excel(pasta_com_excel)
    assert len(df) > 0


def test_ler_excel_colunas_corretas(pasta_com_excel):
    """Deve retornar as colunas exatas do arquivo Excel."""
    df = ler_excel(pasta_com_excel)
    assert list(df.columns) == ['INSTALACAO', 'STATUS', 'MUNICIPIO']


def test_ler_excel_sem_arquivo_lanca_erro(pasta_sem_excel):
    """Deve lançar FileNotFoundError quando não há Excel na pasta."""
    with pytest.raises(FileNotFoundError):
        ler_excel(pasta_sem_excel)
