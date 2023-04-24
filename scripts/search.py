import sys
import os
import pandas as pd
import json
from datetime import datetime
import numpy as np

root_folder = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_folder)

from utils import connSQL,connNoSQL

# def user_summary_relational_data(SQL_client,username,userscreenname,userverification,start_datetime,end_datetime):
#     if not(username):
#         username='%'
#     if not(userscreenname):
#         userscreenname='%'

#     query=f'''SELECT * FROM tweets 
#                 LEFT OUTER JOIN user_profile 
#                 ON tweets.user_id = user_profile.user_id 
#                 WHERE
#                 LOWER(user_profile.name) LIKE LOWER('{username}') 
#                 AND 
#                 LOWER(user_profile.screen_name) LIKE LOWER('{userscreenname}')
#                 AND
#                 tweet_created_at BETWEEN '{start_datetime}' AND '{end_datetime}'
#             '''
    
#     #Appending query for user verified status
#     if(userverification=='2'):
#         query+=''' AND verified=TRUE'''
#     if(userverification=='3'):
#         query+=''' AND verified=FALSE'''
    
#     print(query)

#     #run the query
#     relational_data_df=pd.read_sql_query(query,con=SQL_client)

#     #If no results found
#     if relational_data_df.empty:
#         return(pd.DataFrame({'message': ['sorry no values found']}))
    
#     #Dropping column 'tweet_created_at'
#     relational_data_df=relational_data_df.drop('tweet_created_at', axis=1)
    
    
#     #Tweets ids of same 'tweet_flag' grouped together as list
#     all_columns_except_tweet_id=(relational_data_df.columns.difference(['tweet_id']).to_list())
#     relational_data_df= relational_data_df.groupby(all_columns_except_tweet_id,as_index=False).agg({'tweet_id':lambda x: list(x)})

#     #'tweet_flag' pivoted as columns
#     relational_data_df=pd.concat([relational_data_df.drop(['tweet_id','tweet_flag'], axis = 1) , relational_data_df.pivot(columns = "tweet_flag", values = "tweet_id")], axis = 1).reset_index(drop = True)
    
#     #counting tweets
#     if('original_tweet_flag' in relational_data_df.columns):
#         relational_data_df['original_tweet_count'] = relational_data_df['original_tweet_flag'].apply(lambda x: f'<a value={x} href="http://example.com/">{len(x)}</a>'if isinstance(x, list) else 0)
#     if('quoted_tweet_flag' in relational_data_df.columns):
#         relational_data_df['quoted_tweet_count'] = relational_data_df['quoted_tweet_flag'].apply(lambda x: f'<a value={x} href="http://example.com/">{len(x)}</a>'if isinstance(x, list) else 0)
#     if('reply_tweet_flag' in relational_data_df.columns):
#         relational_data_df['reply_tweet_count'] = relational_data_df['reply_tweet_flag'].apply(lambda x: f'<a value={x} href="http://example.com/">{len(x)}</a>'if isinstance(x, list) else 0)
#     if('retweet_flag' in relational_data_df.columns):
#         relational_data_df['retweet_count'] = relational_data_df['retweet_flag'].apply(lambda x: f'<a value={x} href="http://example.com/">{len(x)}</a>'if isinstance(x, list) else 0)
#     #ordering columns
#     columns_order=['name','screen_name','user_id', 'verified','statuses_count', 'original_tweet_count','retweet_count', 'quoted_tweet_count','reply_tweet_count','followers_count', 'friends_count', 'favourites_count','listed_count','location','language', 'description','url', 'default_profile_image','profile_image_url', 'default_profile', 'profile_background_image_url']
#     relational_data_df = relational_data_df[[col for col in columns_order if col in relational_data_df.columns]]
#     return(relational_data_df)


def fetch_searched_tweet_metadata_user_data(SQL_client,username,userscreenname,userverification,start_datetime,end_datetime,filtered_tweet_ids):
    start_datetime=datetime.strptime(start_datetime, '%m/%d/%y %I:%M %p').strftime('%Y-%m-%d %H:%M:%S')
    end_datetime=datetime.strptime(end_datetime, '%m/%d/%y %I:%M %p').strftime('%Y-%m-%d %H:%M:%S')

    query=f'''SELECT t.*, u.name, u.screen_name,u.verified, 
            (SELECT COUNT(r.retweet_id) FROM retweets r WHERE r.tweet_id = t.tweet_id) AS retweet_count,
            (SELECT COUNT(q.quoted_tweet_id) FROM quoted_tweets q WHERE q.tweet_id = t.tweet_id) AS quoted_count,
            (SELECT COUNT(p.reply_tweet_id) FROM reply p WHERE p.tweet_id = t.tweet_id) AS reply_count
            FROM tweets t
            JOIN user_profile u ON t.user_id = u.user_id
            WHERE
            t.tweet_created_at BETWEEN '{start_datetime}' AND '{end_datetime}'
            '''
    
    if(username):
        query+=f''' AND LOWER(u.name) LIKE LOWER('{username}') '''
    if(userscreenname):
        query+=f''' AND LOWER(u.screen_name) LIKE LOWER('{userscreenname}')'''

    #Appending query for user verified status
    if(userverification=='2'):
        query+=''' AND u.verified=TRUE'''
    if(userverification=='3'):
        query+=''' AND u.verified=FALSE'''
    
    if(len(filtered_tweet_ids)):
         filtered_tweet_ids=tuple(filtered_tweet_ids)
         query+=f''' AND t.tweet_id IN {filtered_tweet_ids}'''
    print(query)

    #run the query
    searched_tweet_metadata_user_data=pd.read_sql_query(query,con=SQL_client)
    return(searched_tweet_metadata_user_data)


def fetch_searched_tweets_data(NoSQL_client,tweetstring,hashtags,tweetsensitivity,tweetcontenttype,start_datetime,end_datetime,filtered_tweet_ids):
    # Define search parameters
    if(hashtags):
        hashtags = hashtags.split(',')
        hashtags=[x.lower() for x in hashtags]
    start_datetime=datetime.strptime(start_datetime, '%m/%d/%y %I:%M %p').timestamp()* 1000
    end_datetime=datetime.strptime(end_datetime, '%m/%d/%y %I:%M %p').timestamp()* 1000

    # Construct Elasticsearch Query DSL
    search_query = {
                    "query": {
                        "bool": {
                        "should": [],
                        "must":[
                        {
                        "range": {
                        "created_at": {
                            "gte": start_datetime,
                            "lte": end_datetime
                            }
                        }
                        }
                        ],
                        "filter": []
                        }
                    },
                    "sort": [
                        {
                        "_score": {
                            "order": "desc"
                        }
                        }
                    ],
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

    # Conditionally include the match query for text
    if(len(tweetstring)):
        search_query["query"]["bool"]["should"].append({
            "match": {
                "text": {
                    "query": tweetstring,
                    "operator": "or",
                    "boost": 3
                }
            }
        })

    # Conditionally include the terms query for entities.hashtags.text
    if(len(hashtags)):
        search_query["query"]["bool"]["should"].append({
            "terms": {
                "entities.hashtags.text": hashtags
            }
        })

    # Conditionally include the terms query for id_str
    if(len(filtered_tweet_ids)):
        search_query["query"]["bool"]["filter"].append({
            "terms": {
            "id_str": filtered_tweet_ids
            }
        })

    #Condition that atleast one of tweetstring or hashtags should match if provided. 
    if(len(tweetstring) or len(hashtags)):
        search_query["query"]["bool"]["minimum_should_match"]=1

    # Conditionally include the filter query for possibly_sensitive
    if(tweetsensitivity) == "2":
        search_query["query"]["bool"]["filter"].append({"term": {"possibly_sensitive": 1}})
    if(tweetsensitivity) == "3":
        search_query["query"]["bool"]["filter"].append({"bool": {"must_not": {"term": {"possibly_sensitive": 1}}}})

    # Convert query to JSON string
    search_query_json = json.dumps(search_query)
    print(search_query_json)

    # Execute the search query and retrieve the sorted results
    response = NoSQL_client.search(index="index__*", body=search_query_json)
    sorted_hits = response["hits"]["hits"]
    searched_tweets_data = pd.DataFrame.from_records(sorted_hits)
    
    if(searched_tweets_data.empty):
        return(searched_tweets_data)
    ## Extracting required columns from result dataframe
    
    # Extracting  'possibly_sensitive'
    searched_tweets_data['possibly_sensitive'] = searched_tweets_data['_source'].apply(lambda x:x['possibly_sensitive'])
    
    # Extracting 'id_str'
    searched_tweets_data['id_str'] = searched_tweets_data['_source'].apply(lambda x:x['id_str'])

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
        
    searched_tweets_data['hashtags'] = searched_tweets_data['_source'].apply(get_hashtags)
    
    # Extracting text
    searched_tweets_data['text'] = searched_tweets_data['_source'].apply(lambda x:x['text'])

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
    searched_tweets_data['media_type'] = searched_tweets_data['_source'].apply(get_media_type)

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
        
    
    searched_tweets_data['media_url'] = searched_tweets_data['_source'].apply(get_media_url)


    # Create a list of desired columns
    desired_columns = ['id_str', 'text', 'hashtags','_score', 'possibly_sensitive','media_type','media_url']

    #Filtering desired columns from dataframe
    existing_desired_columns = [col for col in desired_columns if col in searched_tweets_data.columns]
    searched_tweets_data = searched_tweets_data[existing_desired_columns]
    
    #Renaming tweet id column
    searched_tweets_data = searched_tweets_data.rename(columns={'id_str': 'tweet_id'})
    return(searched_tweets_data)

def fetch_results(username,userscreenname,userverification,tweetstring,hashtags,tweetsensitivity,tweetcontenttype,start_datetime,end_datetime):
    SQL_client=connSQL()
    NoSQL_client=connNoSQL()
    filtered_tweet_ids=[]

    #If user parameters are provided. Search relatinal->non-relational
    if(username or userscreenname):
        searched_tweet_metadata_user_data=fetch_searched_tweet_metadata_user_data(SQL_client,username,userscreenname,userverification,start_datetime,end_datetime,filtered_tweet_ids)
        if(searched_tweet_metadata_user_data.empty):
            return(searched_tweet_metadata_user_data)
        filtered_tweet_ids = searched_tweet_metadata_user_data['tweet_id'].values.tolist()
        filtered_tweet_ids=[str(x) for x in filtered_tweet_ids]
        searched_tweets_data=fetch_searched_tweets_data(NoSQL_client,tweetstring,hashtags,tweetsensitivity,tweetcontenttype,start_datetime,end_datetime,filtered_tweet_ids)
        if(searched_tweets_data.empty):
            return(searched_tweets_data)

    #If user parameters are non-provided. Search non-relatinal->relational
    elif(tweetstring or hashtags):
        searched_tweets_data=fetch_searched_tweets_data(NoSQL_client,tweetstring,hashtags,tweetsensitivity,tweetcontenttype,start_datetime,end_datetime,filtered_tweet_ids)
        if(searched_tweets_data.empty):
            return(searched_tweets_data)
        filtered_tweet_ids = searched_tweets_data['tweet_id'].values.tolist()
        filtered_tweet_ids=[str(x) for x in filtered_tweet_ids]
        searched_tweet_metadata_user_data=fetch_searched_tweet_metadata_user_data(SQL_client,username,userscreenname,userverification,start_datetime,end_datetime,filtered_tweet_ids)
        if(searched_tweet_metadata_user_data.empty):
            return(searched_tweet_metadata_user_data)
    
    else:
        return(pd.DataFrame(['Please provide one of -userid,username,tweetstring,hashtags'], columns=['Message']))
  
    results_df=pd.merge(searched_tweets_data,searched_tweet_metadata_user_data, on='tweet_id', how='inner') 
    #results_df=searched_tweet_metadata_user_data
    return(results_df)

if __name__=='__main__':
    fetch_results()
