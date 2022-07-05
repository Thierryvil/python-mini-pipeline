import argparse
import numpy as np
from log import Log
from os.path import join, dirname
from sys import argv
import pandas as pd
from dotenv import load_dotenv
from infra.mysql.mysql_adapter import MySQLAdapter, MySQLConfig
from os import environ
import re

log = Log(join(dirname(argv[0]), 'log'))
parser = argparse.ArgumentParser()
parser.add_argument('--input', '-i', required=True)
args = parser.parse_args()


def remove_specials_characters(value: str):
    return re.sub(r'[a-z\W+]', '', value) if value else value


def convert_string_to_int(value: str):
    try:
        return int(value) if value else 0
    except ValueError:
        return 0


def format_sate(value: str):
    return value[0:2].upper() if value else value


def format_name(value: str):
    return value[0:50] if value else value


def clean_columns(row: pd.Series):
    row['Nome'] = format_name(row['Nome'])
    row['Estado'] = format_sate(row['Estado'])
    row['Tamanho minimo do projeto'] = remove_specials_characters(row['Tamanho minimo do projeto'])
    row['Tamanho minimo do projeto'] = convert_string_to_int(row['Tamanho minimo do projeto'])
    row['Custo mínimo por hora'] = remove_specials_characters(row['Custo mínimo por hora'])
    row['Custo mínimo por hora'] = convert_string_to_int(row['Custo mínimo por hora'])
    row['Custo máximo por hora'] = remove_specials_characters(row['Custo máximo por hora'])
    row['Custo máximo por hora'] = convert_string_to_int(row['Custo máximo por hora'])
    row['Número máximo de empregados'] = remove_specials_characters(row['Número máximo de empregados'])
    row['Número máximo de empregados'] = convert_string_to_int(row['Número máximo de empregados'])
    row['Número mínimo de empregados'] = remove_specials_characters(row['Número mínimo de empregados'])
    row['Número mínimo de empregados'] = convert_string_to_int(row['Número mínimo de empregados'])
    row['Porcentagem de serviços de IA'] = int(row['Porcentagem de serviços de IA'].replace('%', ''))
    return row


try:
    log.info(f'Carregando as variaveis de ambiente.')
    load_dotenv()
    mysql_configs = MySQLConfig(
        password=environ.get('MYSQL_ROOT_PASSWORD'),
        database_name=environ.get('MYSQL_DATABASE'),
    )

    log.info(f'Carregando o arquivo {args.input}.')
    empresas = pd.read_csv(args.input)

    log.info('Removendo duplicados.')
    empresas.drop_duplicates(inplace=True)

    log.info('Divindo a coluna Localização')
    empresas[['Cidade', 'Estado', 'Pais']] = empresas['Localização'].str.split(',', expand=True)
    empresas['Cidade'] = empresas['Cidade'].replace(['', np.NaN], None)
    empresas['Estado'] = empresas['Estado'].str.strip()
    empresas['Estado'] = empresas['Estado'].replace(['', np.NaN], None)
    empresas['Pais'] = empresas['Pais'].str.strip()
    empresas['Pais'] = empresas['Pais'].replace(['', np.NaN], 'EUA')

    log.info('Divindo a coluna Custo Médio por hora.')
    empresas[['Custo mínimo por hora', 'Custo máximo por hora']] = empresas['Custo médio por hora'].str.split(
        '-', expand=True)

    log.info('Divindo a coluna Numero de empregados.')
    empresas[['Número mínimo de empregados', 'Número máximo de empregados']] = (
        empresas['Numero de empregados'].str.split('-', expand=True)
    )

    log.info('Limpando o arquivo.')
    empresas = empresas.apply(clean_columns, axis=1)

    log.info('Removendo colunas inutilizaveis')
    empresas.drop(['Localização', 'Custo médio por hora', 'Numero de empregados'], axis=1, inplace=True)

    log.info('Reorganizando as colunas.')
    columns = [
        'Nome', 'Website', 'Cidade', 'Estado', 'Pais', 'Tamanho minimo do projeto',
        'Porcentagem de serviços de IA',	'Custo mínimo por hora', 'Custo máximo por hora',
        'Número mínimo de empregados', 'Número máximo de empregados'
    ]
    empresas = empresas[columns]

    with MySQLAdapter(mysql_configs, log) as mysql:
        empresas = [tuple(empresa) for empresa in empresas.to_numpy()]
        log.info('[MYSQL] Inserindo registros no banco de dados.')
        query = (
            "INSERT INTO companies ("
            "name, "
            "site, "
            "city, "
            "state, "
            "country, "
            "project_min_size, "
            "min_cost_hour, "
            "max_cost_hour, "
            "min_employee_size, "
            "max_employee_size, "
            "AI_service_percent "
            ") "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        mysql.executemany(query, empresas)
        mysql.execute("""commit""")
except BaseException as e:
    log.error(f'ERROR: {e}')
finally:
    log.info(f'{ "-" * 5} PROCESSO FINALIZADO. {"-" * 5}')


