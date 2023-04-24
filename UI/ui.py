from flask import Flask, render_template, request
import sys
import os
import pandas as pd
import numpy as np
import time

root_folder = os.path.abspath(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root_folder)

from scripts.search import fetch_results

app = Flask(__name__)

@app.route('/')
def search():
    return render_template('search.html')

@app.route('/explore')
def explore():
    return render_template('explore.html')

@app.route('/handle_data', methods=['POST'])
def handle_data():
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
    results_df=fetch_results(username,userscreenname,userverification,tweetstring,hashtags,tweetsensitivity,tweetcontenttype,start_datetime,end_datetime)
    search_end_time=time.time()
    total_time_taken={search_end_time-search_start_time}
    if(results_df.empty):
        results_df = pd.DataFrame(['no results found'], columns=['Message'])
    results_df.index = np.arange(1, len(results_df)+1)
    return render_template('results.html',results=results_df.to_html(escape=False),performanceresults=[total_time_taken])


if __name__ == "__main__":
    app.run(port=8000,debug=True)
