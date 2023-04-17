"""
File : /scripts/utils.py

Description:
1. Setup a connection to the PostgreSQL database
"""
from configparser import ConfigParser
import psycopg2
from elasticsearch import Elasticsearch

config = ConfigParser()
config.read('config.ini')

def connSQL():
    _connection = psycopg2.connect(
        **dict(config.items('postgres'))
    )

    return _connection

def connNoSQL():
    esConfig = dict(config.items('elasticsearch'))
    
    _client = Elasticsearch(
        f"http://{esConfig['host']}:{esConfig['port']}",
        basic_auth = (esConfig['elastic_username'], esConfig['elastic_password']),
        max_retries=3,
        retry_on_timeout=True
    )

    return _client

def removeNoSQLData():
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
