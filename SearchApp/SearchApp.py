from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/')
def search():
    return render_template('search.html')

@app.route('/explore')
def explore():
    return render_template('explore.html')

@app.route('/handle_data', methods=['POST'])
def handle_data():
    username = str(request.form['username'])
    userid= str(request.form['userid'])
    userverification= str(request.form['userverification'])
    tweetstring= str(request.form['tweetstring'])
    hashtags= str(request.form['hashtags'])
    tweetsensitivity = str(request.form['tweetsensitivity'])
    tweetcontenttype= str(request.form['tweetcontenttype'])
    datetimerange= str(request.form['datetimerange'])
    performancestats= str(request.form['performancestats'])
    data=[username,userid,userverification,tweetstring,hashtags,tweetsensitivity,tweetcontenttype,datetimerange,performancestats]
    return render_template('test.html')

if __name__ == "__main__":
    app.run(debug=True)