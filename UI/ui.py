from flask import Flask, render_template, request
import sys
import os
import pandas as pd
import numpy as np
import time
import asyncio

root_folder = os.path.abspath(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root_folder)

from scripts.search import fetch_results
from scripts.user_search import fetch_user_results
from scripts.tweet_search import fetch_tweet_results
from scripts.metric_search import fetch_metric_results
from scripts.cache import CacheManager
from scripts.utils import pushLogs

app = Flask(__name__)

@app.route('/')
def search():
    return render_template('search.html')

@app.route('/toplevelmetrics')
def topmetrics():
    return render_template('toplevelmetrics.html')

@app.route('/explore')
def explore():
    return render_template('explore.html')

@app.route('/handle_data', methods=['POST'])
def handle_data():
    cacher = CacheManager()
    cache_flag=0

    form_data = request.form.to_dict(flat=False)

    #UI Parameters
    username = str(request.form['username'])
    userscreenname= str(request.form['userscreenname']).strip()
    userverification= str(request.form['userverification']).strip()
    tweetstring= str(request.form['tweetstring']).strip()
    form_data['hashtags'] = form_data['hashtags'][1:]
    hashtags = form_data['hashtags'][0]
    tweetsensitivity = str(request.form['tweetsensitivity']).strip()
    tweetcontenttype= str(request.form['tweetcontenttype']).strip()
    datetimerange= str(request.form['datetimerange']).strip()
    start_datetime=datetimerange.split('-')[0].strip()
    end_datetime=datetimerange.split('-')[1].strip()
  
    search_start_time=time.time()
    if cacher.__contains__(form_data):
        cache_result = cacher.getQuery(form_data)
        cache_flag=1
        results_df=pd.DataFrame(cache_result)
    else:
        results_df=fetch_results(username,userscreenname,userverification,tweetstring,hashtags,tweetsensitivity,tweetcontenttype,start_datetime,end_datetime)
    search_end_time=time.time()
    total_time_taken=search_end_time-search_start_time
    print(form_data)
    if(results_df.empty):
        results_df = pd.DataFrame(['no results found'], columns=['Message'])
    results_df.index = np.arange(1, len(results_df)+1)
    if(cache_flag==0):
        cacher.putQuery(key = form_data, result = results_df.to_dict(orient='records'), response_time = total_time_taken)
    cacher.close(save = True)
    asyncio.run(pushLogs(form_data, results_df.to_dict(orient='records'), total_time_taken))
    return render_template('results.html',results=results_df.to_html(escape=False,classes="table table-striped table-bordered"),performanceresults=[total_time_taken])


@app.route('/handle_user', methods=['GET','POST'])
def handle_user():
    args = request.args
    user_id= (args.get("value"))
    search_start_time=time.time()
    (results1_df,results2_df,results3_df,results4_df) = fetch_user_results(user_id)
    search_end_time=time.time()
    total_time_taken={search_end_time-search_start_time}
    return render_template('user_results.html',results=(formatted_df(results1_df),formatted_df(results2_df),formatted_df(results3_df),formatted_df(results4_df)),performanceresults=[total_time_taken])

@app.route('/handle_tweet', methods=['GET','POST'])
def handle_tweet():
    args = request.args
    my_arg1= (args.get("value"))    
    my_arg2= (args.get("key"))
    search_start_time=time.time()
    if(my_arg2=='all'):
        (retweet_results_df,quoted_results_df,reply_tweets_df)=fetch_tweet_results(my_arg1,my_arg2)
        search_end_time=time.time()
        total_time_taken={search_end_time-search_start_time}
        return render_template('all_tweet_results.html',results=(formatted_df(retweet_results_df),formatted_df(quoted_results_df),formatted_df(reply_tweets_df)),performanceresults=[total_time_taken])
    else:
        results_df=fetch_tweet_results(my_arg1,my_arg2)
        search_end_time=time.time()
        total_time_taken={search_end_time-search_start_time}
        return render_template('results.html',results=formatted_df(results_df),performanceresults=[total_time_taken])

@app.route('/handle_topmetrics', methods=['POST'])
def handle_topmetrics():
    selected_option = request.form['mySelect']
    search_start_time=time.time()
    results_df=fetch_metric_results(selected_option)
    search_end_time=time.time()
    total_time_taken={search_end_time-search_start_time}
    return render_template('metricresults.html',results=formatted_df(results_df),performanceresults=[total_time_taken])

def formatted_df(my_df):
    print(type(my_df))
    print(my_df)
    if(my_df.empty):
        return('No Results')
    my_df.index = np.arange(1, len(my_df)+1)
    my_df=my_df.to_html(escape=False,classes="table table-striped table-bordered")
    return(my_df)


if __name__ == "__main__":
    app.run(port=8000,debug=True)

