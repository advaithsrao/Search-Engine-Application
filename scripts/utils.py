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
    connection = psycopg2.connect(
        **dict(config.items('postgres'))
    )

    return connection



def connNoSQL():
    esConfig = dict(config.items('elasticsearch'))
    
    client = Elasticsearch(
        f"http://{esConfig['host']}:{esConfig['port']}",
        basic_auth = (esConfig['elastic_username'], esConfig['elastic_password'])
    )

    return client
