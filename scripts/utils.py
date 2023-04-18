"""
File : /scripts/utils.py

Description:
1. Setup a connection to the PostgreSQL database
"""
import os
from configparser import ConfigParser
import psycopg2
from elasticsearch import Elasticsearch


def getConfig():
    _cwd = os.getcwd()

    _abspath = os.path.abspath(__file__)
    _dname = os.path.dirname(_abspath)
    os.chdir(_dname)

    _config = ConfigParser()
    _config.read('../config.ini')

    os.chdir(_cwd)

    return _config


def connSQL():
    config = getConfig()
    
    _connection = psycopg2.connect(
        **dict(config.items('postgres'))
    )

    return _connection

def connNoSQL():
    config = getConfig()

    esConfig = dict(config.items('elasticsearch'))
    
    _client = Elasticsearch(
        f"http://{esConfig['host']}:{esConfig['port']}",
        basic_auth = (esConfig['elastic_username'], esConfig['elastic_password']),
        max_retries=3,
        retry_on_timeout=True
    )

    return _client

def removeNoSQLData():
    config = getConfig()

    esConfig = dict(config.items('elasticsearch'))
    
    _client = Elasticsearch(
        f"http://{esConfig['host']}:{esConfig['port']}",
        basic_auth = (esConfig['elastic_username'], esConfig['elastic_password'])
    )

    try:
        _client.indices.delete(index='_all')
        print('Elastic Search: *** Index Data Delete Successful ***')
    except Exception as e:
        print(f'Elastic Search: *** Index Data Delete Unsuccessful as {e} ***')
