import sys
import os
import pandas as pd
root_folder = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_folder)

from utils import connSQL,connNoSQL

def fetch_relational_data(SQL_client,username,userscreenname):
    if not(username):
        username='%'
    if not(userscreenname):
        userscreenname='%'

    query=f'''SELECT * FROM tweets 
                LEFT OUTER JOIN user_profile 
                ON tweets.user_id = user_profile.user_id 
                WHERE
                LOWER(user_profile.name) LIKE LOWER('{username}') 
                AND 
                LOWER(user_profile.screen_name) LIKE LOWER('{userscreenname}')
            '''
    relational_data_df=pd.read_sql_query(query,con=SQL_client)
    relational_data_df=relational_data_df.drop('tweet_created_at', axis=1)
    all_columns_except_tweet_id=(relational_data_df.columns.difference(['tweet_id']).to_list())
    relational_data_df= relational_data_df.groupby(all_columns_except_tweet_id,as_index=False).agg({'tweet_id':lambda x: list(x)})
    relational_data_df=pd.concat([relational_data_df.drop(['tweet_id','tweet_flag'], axis = 1) , relational_data_df.pivot(columns = "tweet_flag", values = "tweet_id")], axis = 1).reset_index(drop = True)
    relational_data_df['original_tweet_count'] = relational_data_df['original_tweet_flag'].apply(lambda x: f'<a value={x} href="http://example.com/">{len(x)}</a>'if isinstance(x, list) else 0)
    relational_data_df['quoted_tweet_count'] = relational_data_df['quoted_tweet_flag'].apply(lambda x: f'<a value={x} href="http://example.com/">{len(x)}</a>'if isinstance(x, list) else 0)
    relational_data_df['reply_tweet_count'] = relational_data_df['reply_tweet_flag'].apply(lambda x: f'<a value={x} href="http://example.com/">{len(x)}</a>'if isinstance(x, list) else 0)
    relational_data_df['retweet_count'] = relational_data_df['retweet_flag'].apply(lambda x: f'<a value={x} href="http://example.com/">{len(x)}</a>'if isinstance(x, list) else 0)
    columns_order=['name','screen_name','user_id', 'verified','statuses_count', 'original_tweet_count','retweet_count', 'quoted_tweet_count','reply_tweet_count','followers_count', 'friends_count', 'favourites_count','listed_count','location','language', 'description','url', 'default_profile_image','profile_image_url', 'default_profile', 'profile_background_image_url']
    relational_data_df = relational_data_df[columns_order]
    return(relational_data_df)


def fetch_results(username,userscreenname,userverification,tweetstring,hashtags,tweetsensitivity,tweetcontenttype,datetimerange,performancestats):
    SQL_client=connSQL()
    NoSQL_client=connNoSQL()
    relational_data_df=fetch_relational_data(SQL_client,username,userscreenname)
    if(username or username):
        pass
    #No SQL Query
    #es_body={'query':{"match_all": {}}}
    #resp = NoSQL_client.search(index="index_*",body=es_body)
    #print("Got %d Hits:" % resp['hits']['total']['value'])
    #for hit in resp['hits']['hits']:
        #print(hit["_source"])
    results_df=relational_data_df
    return(results_df)

if __name__=='__main__':
    fetch_results()
