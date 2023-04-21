from flask import Flask, render_template, request
import sys
import os

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
    #UI Parameters
    username = str(request.form['username']).strip()
    userscreenname= str(request.form['userscreenname']).strip()
    userverification= str(request.form['userverification']).strip()
    tweetstring= str(request.form['tweetstring']).strip()
    hashtags= str(request.form['hashtags']).strip()
    tweetsensitivity = str(request.form['tweetsensitivity']).strip()
    tweetcontenttype= str(request.form['tweetcontenttype']).strip()
    datetimerange= str(request.form['datetimerange']).strip()
    if('performancestats' in request.form):
        performancestats = 1
    else:
        performancestats = 0
    results_df=fetch_results(username,userscreenname,userverification,tweetstring,hashtags,tweetsensitivity,tweetcontenttype,datetimerange,performancestats)

    return render_template('results.html',results=results_df.to_html(escape=False))

if __name__ == "__main__":
    app.run(port=8000,debug=True)