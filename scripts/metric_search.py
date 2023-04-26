import sys
import os
import pandas as pd
import json
from datetime import datetime
import numpy as np

root_folder = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_folder)

from utils import connSQL,connNoSQL


def top_users_by_followers(SQL_client):
    query=f'''SELECT u.*
            FROM user_profile u
			ORDER BY u.followers_count DESC LIMIT 10'''
    results_df=pd.read_sql_query(query,con=SQL_client)
    return(results_df)

def top_tweets_by_retweets(SQL_client,NoSQL_client):
    query1=f'''SELECT tweet_id,count(retweet_id) as retweet_count           
        FROM retweets r
		group by tweet_id
		order by retweet_count
        '''
    df1=pd.read_sql_query(query1,con=SQL_client)
    if(df1.empty):
            return(df1)
    filtered_tweet_ids = df1['tweet_id'].values.tolist()
    filtered_tweet_ids=[str(x) for x in filtered_tweet_ids]

    query2 = {
                "query": {
                    "bool": {
                    "should": [],
                    "must":[
                    ],
                    "filter": []
                    }
                },
                "_source": [
                    "id_str",
                    "text",
                    "entities.hashtags.text",
                    "possibly_sensitive",
                    "entities.media.type",
                    "entities.media.url"
                ],
                "size":10000
                }
    
    # Conditionally include the terms query for id_str
    if(len(filtered_tweet_ids)):
        query2["query"]["bool"]["filter"].append({
            "terms": {
            "id_str": filtered_tweet_ids
            }
        })
    
        # Convert query to JSON string
    search_query_json = json.dumps(query2)
    print(search_query_json)

    # Execute the search query and retrieve the sorted results
    response = NoSQL_client.search(index="index__*", body=search_query_json)
    sorted_hits = response["hits"]["hits"]
    df2 = pd.DataFrame.from_records(sorted_hits)
    
    if(df2.empty):
        return(df2)
    ## Extracting required columns from result dataframe
    
    # Extracting  'possibly_sensitive'
    df2['possibly_sensitive'] = df2['_source'].apply(lambda x:x['possibly_sensitive'])
    
    # Extracting 'id_str'
    df2['id_str'] = df2['_source'].apply(lambda x:x['id_str'])

    # Extracting hashtags
    def get_hashtags(row):
        text_list = []
        if('entities' in row.keys()):
            if('hashtags' in row['entities']):
                hash_list = row['entities']['hashtags']
                for i in hash_list:
                    text_list.append(i['text'])
                return text_list
            else:
                return np.nan
        else:
            return np.nan
        
    df2['hashtags'] = df2['_source'].apply(get_hashtags)
    
    # Extracting text
    df2['text'] = df2['_source'].apply(lambda x:x['text'])

    # Extracting 'media_type'
    def get_media_type(row):
        if 'entities' in row.keys():
            if 'media' in row['entities']:
                if 'type' in row['entities']['media'][0].keys():
                    return row['entities']['media'][0]['type']
                else:
                    return np.nan
            else:
                return np.nan
        else:
            return np.nan
    df2['media_type'] = df2['_source'].apply(get_media_type)

    # Extracting 'media_url'
    def get_media_url(row):
        if 'entities' in row.keys():
            if 'media' in row['entities']:
                if 'url' in row['entities']['media'][0].keys():
                    return row['entities']['media'][0]['url']
                else:
                    return np.nan
            else:
                return np.nan
        else:
            return np.nan
        
    
    df2['media_url'] = df2['_source'].apply(get_media_url)


    # Create a list of desired columns
    desired_columns = ['id_str', 'text', 'hashtags','retweet_count ', 'possibly_sensitive','media_type','media_url']

    #Filtering desired columns from dataframe
    existing_desired_columns = [col for col in desired_columns if col in df2.columns]
    df2 = df2[existing_desired_columns]
    
    #Renaming tweet id column
    df2 = df2.rename(columns={'id_str': 'tweet_id'})
    results_df=pd.merge(df1,df2, on='tweet_id', how='inner') 
    results_df=results_df.sort_values(['retweet_count'],ascending=False)
    return(results_df.head(10))


def fetch_metric_results(option):
    SQL_client=connSQL()
    NoSQL_client=connNoSQL()
    if(option=='1'):
        return(top_users_by_followers(SQL_client))
    elif(option=='2'):
        return(top_tweets_by_retweets(SQL_client,NoSQL_client))
