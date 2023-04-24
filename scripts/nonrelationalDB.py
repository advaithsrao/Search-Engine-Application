"""
File : /scripts/nonrelationalDB.py

Description:
1. Create a elasticsearch database with required indices
2. Push a dataset into the created indices
"""
from utils import connNoSQL
import pandas as pd
import json

import warnings
warnings.filterwarnings('ignore')

def createIndexPushData(_client):
    doc_count = 0
    base_index_name = 'index_'
    max_docs_per_index = 5000

    # Loop through the documents and index them
    for doc in docs:
        try:
            # Compute the index name based on the counter variable
            index_name = f"{base_index_name}_{doc_count // max_docs_per_index}"
            
            # Check if the index exists, and create if it doesn't
            if not _client.indices.exists(index = index_name):
                _client.indices.create(
                    index = index_name
                )
            
            # Index the document in the current index
            _client.index(
                index = index_name, 
                body = doc
            )
            
            # Increment the counter variable
            doc_count += 1

            if doc_count % max_docs_per_index == 0:
                print(f'\t Total Index Push done for {doc_count}')
        except Exception as e:
            print(f'\t Index Push Unsuccessful for doc_count: {doc_count} as {e}')


if __name__ == "__main__":
    
    client = connNoSQL()
    
    # Create Indices and Push Data
    print('ELASTICSEARCH: *** Index Creation and Push Started ***')
    
    # Load the Twitter data
    twitterdf = pd.concat(
        [
            pd.read_json("./data/corona-out-2", lines=True),
            pd.read_json("./data/corona-out-3", lines=True)
        ]
    )

    # Create new required columns
    twitterdf["user_id"] = twitterdf["user"].apply(
        lambda x: x["id_str"]
    )

    # Reset indices
    twitterdf.reset_index(
        inplace = True, 
        drop = True
    )

    twitterdf = twitterdf[
        [
            'contributors',
            'text',
            'source',
            'id_str',
            'created_at',
            'user_id',
            'truncated',
            'lang',
            'quote_count',
            'reply_count',
            'retweet_count',
            'favorite_count',
            'favorited',
            'retweeted',
            'possibly_sensitive',
            'withheld_in_countries',
            'place',
            'entities',
            'extended_entities',
            'quoted_status',
            'retweeted_status'
        ]
    ]

    twitterdf['place'] = twitterdf['place'].map(str)
    twitterdf['id_str'] = twitterdf['id_str'].map(str)
    # twitterdf['entities'] = twitterdf['entities'].map(str)
    twitterdf['extended_entities'] = twitterdf['extended_entities'].map(str)
    twitterdf['quoted_status'] = twitterdf['quoted_status'].map(str)
    twitterdf['retweeted_status'] = twitterdf['retweeted_status'].map(str)
    
    # Create document list
    docs = json.loads(
        twitterdf.to_json(
            orient='records'
        )
    )

    # Call Index Creation and push function
    try:
        createIndexPushData(client)
        print('ELASTICSEARCH: *** All Indices Created and Data Successfully Pushed ***')
    except Exception as e:
        print(f'ELASTICSEARCH: *** Index Push Unsuccessful  as {e} ***')
