import sys
import os

root_folder = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_folder)

from utils import connSQL,connNoSQL

def fetch_results():
    SQL_client=connSQL()
    NoSQL_client=connNoSQL()
    SQL_cursor=SQL_client.cursor()

    #SQL Query
    SQL_cursor.execute('SELECT tablename FROM pg_catalog.pg_tables;')
    for table_name in SQL_cursor:
        print(table_name)
    
    #No SQL Query
    #es_body={'query':{"match_all": {}}}
    #resp = NoSQL_client.search(index="index_*",body=es_body)
    #print("Got %d Hits:" % resp['hits']['total']['value'])
    #for hit in resp['hits']['hits']:
        #print(hit["_source"])


if __name__=='__main__':
    fetch_results()
