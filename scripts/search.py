import sys
import os
import pandas as pd
import json
from datetime import datetime
import numpy as np

root_folder = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_folder)

from utils import connSQL,connNoSQL


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
            (t.tweet_flag='original_tweet' OR t.tweet_flag='quoted_tweet')
            AND 
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
        if(len(filtered_tweet_ids)==1):
            filtered_tweet_ids='('+filtered_tweet_ids[0]+')'
        else:
            filtered_tweet_ids=tuple(filtered_tweet_ids)
        query+=f''' AND t.tweet_id IN {filtered_tweet_ids}'''

    #print(query)

    #run the query
    searched_tweet_metadata_user_data=pd.read_sql_query(query,con=SQL_client)
    print('########')
    print(searched_tweet_metadata_user_data)
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
                        "must_not": [
                            {
                            "regexp": {
                                "text": {
                                "value": "#[a-zA-Z]+"
                                }
                            }
                            }
                        ]
                        ,
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
            "match_phrase": {
                "text": {
                    "query": tweetstring,
                     "slop": 1000
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
    if(tweetsensitivity == "2"):
        search_query["query"]["bool"]["filter"].append({"term": {"possibly_sensitive": 1}})
    if(tweetsensitivity == "3"):
        search_query["query"]["bool"]["filter"].append({"bool": {"must_not": {"term": {"possibly_sensitive": 1}}}})
    
    # Conditionally include the filter query for media_type
    if(tweetcontenttype == "2"):
     search_query["query"]["bool"]["filter"].append({"exists": {"field": "entities.media.type"}})

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
        return(pd.DataFrame(['Please provide one of username,userscreenname,tweetstring,hashtags'], columns=['Message']))
  
    results_df=pd.merge(searched_tweets_data,searched_tweet_metadata_user_data, on='tweet_id', how='inner') 
    results_df=results_df.sort_values(by=['_score','retweet_count'], ascending=False)
    results_df=apply_formatting(results_df)
    return(results_df)



def my_tweet_formatting(val1,val2,val3):
    my_frmt = f'''<a href="#" target="_blank" 
                      onclick="window.open('handle_tweet?value={val1}&key={val2}')">{val3}</a>'''
    return(my_frmt)

def my_user_formatting(val1,val2):
    my_frmt = f'''<a href="#" target="_blank" 
                      onclick="window.open('handle_user?value={val1}')">{val2}</a>'''
    return(my_frmt)

def apply_formatting(results_df):
    if('name' in results_df.columns and 'user_id' in results_df.columns):
        results_df['name']=results_df.apply(lambda x:my_user_formatting(x['user_id'],x['name']),axis=1)
    if('screen_name' in results_df.columns and 'user_id' in results_df.columns):
        results_df['screen_name']=results_df.apply(lambda x:my_user_formatting(x['user_id'],x['screen_name']),axis=1)
    if('user_id' in results_df.columns):
        results_df['user_id']=results_df.apply(lambda x:my_user_formatting(x['user_id'],x['user_id']),axis=1)

    if('retweet_count' in results_df.columns and 'tweet_id' in results_df.columns):
        results_df['retweet_count']=results_df.apply(lambda x:my_tweet_formatting(x['tweet_id'],'retweet',x['retweet_count']),axis=1)
    if('quoted_count' in results_df.columns):
        results_df['quoted_count']=results_df.apply(lambda x:my_tweet_formatting(x['tweet_id'],'quoted',x['quoted_count']),axis=1)
    if('reply_count' in results_df.columns):
        results_df['reply_count']=results_df.apply(lambda x:my_tweet_formatting(x['tweet_id'],'reply',x['reply_count']),axis=1)
    if('tweet_id' in results_df.columns):
        results_df['tweet_id']=results_df.apply(lambda x:my_tweet_formatting(x['tweet_id'],'all',x['tweet_id']),axis=1)
    return(results_df)
