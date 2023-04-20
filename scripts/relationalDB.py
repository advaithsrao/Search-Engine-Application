"""
File : /scripts/relationalDB.py

Description:
1. Create a postgres database with desired tables
2. Push a dataset into our postgres database
"""
from utils import connSQL
import pandas as pd
from psycopg2 import extensions
import warnings
warnings.filterwarnings('ignore')

def createPostgresTables(_cursor):
    try:
        # User profile table -> table 1
        _cursor.execute(
            """
            CREATE TABLE user_profile
            (
                user_id TEXT PRIMARY KEY,
                name VARCHAR(50),
                screen_name VARCHAR(15),
                url TEXT NULL ,
                location TEXT NULL,
                followers_count INTEGER NULL,
                friends_count INTEGER,
                listed_count INTEGER,
                favourites_count INTEGER,
                statuses_count INTEGER,
                created_at TIMESTAMP,
                description TEXT,
                language CHAR(4),
                verified BOOLEAN,
                profile_image_url TEXT,
                profile_background_image_url TEXT NULL,
                default_profile BOOLEAN NULL,
                default_profile_image BOOLEAN NULL
            )
            """
        )
        print('\t Table user_profile Create Successful')
    except Exception as e:
        print(f'\t Table user_profile Create Unuccessful as {e}')

    try:
        # Reply tweet table -> table 2
        _cursor.execute(
            """
            CREATE TABLE reply
            (
                reply_tweet_id TEXT PRIMARY KEY,
                tweet_id TEXT
            )
            """
        )
        print('\t Table reply Create Successful')
    except Exception as e:
        print(f'\t Table reply Create Unuccessful as {e}')

    try:
        # Quoted tweet table -> table 3
        _cursor.execute(
            """
            CREATE TABLE quoted_tweets
            (
                quoted_tweet_id TEXT PRIMARY KEY,
                tweet_id TEXT
            )
            """
        )
        print('\t Table quoted_tweets Create Successful')
    except Exception as e:
        print(f'\t Table quoted_tweets Create Unuccessful as {e}')

    try:
        # Retweet table -> table 4
        _cursor.execute(
            """
            CREATE TABLE retweets
            (
                retweet_id TEXT PRIMARY KEY,
                tweet_id TEXT
            )
            """
        )
        print('\t Table retweets Create Successful')
    except Exception as e:
        print(f'\t Table retweets Create Unuccessful as {e}')

    try:
        # Tweets table -> table 5
        _cursor.execute(
            """
            CREATE TABLE tweets
            (
                tweet_id TEXT PRIMARY KEY,
                user_id TEXT,
                tweet_flag VARCHAR(20)
            )
            """
        )
        print('\t Table tweets Create Successful')
    except Exception as e:
        print(f'\t Table tweets Create Unuccessful as {e}')

def preparePushData(_columns, _data, _tablename):
    # print(f'\t Executing Table Push for Table -> {_tablename}')

    _columns = ', '.join(_columns)
    _values = _data.values.tolist()
    
    # Push data into our table, row by row 
    for _value in _values:
        _row = str(
            _value
        ).strip(
            ']['
        ).replace(
            "None", 
            "NULL"
        )
            
        cur.execute(
            f"""
                INSERT INTO
                    {_tablename} ({_columns})
                VALUES
                    ({_row})
            """
        )
    print(f'\t Push for Table -> {_tablename} Successful')

def pushPostgresData(_cursor, _data):
    try:
        # Table 1
        table1Columns = [
            'user_id',
            'name',
            'screen_name',
            'url',
            'location',
            'followers_count',
            'friends_count',
            'listed_count',
            'favourites_count',
            'statuses_count',
            'created_at',
            'description',
            'language',
            'verified',
            'profile_image_url',
            'profile_background_image_url',
            'default_profile',
            'default_profile_image'
        ]

        _data1 = pd.DataFrame(_data['user'].values.tolist())
        
        _data1['description'] = _data1['description'].apply(
            lambda x: x.encode('ascii', 'ignore').decode('utf-8') if bool(x) else x
        )
        
        _data1['description'] = _data1['description'].apply(
            lambda x: extensions.adapt(x).getquoted().decode('utf-8')
        )
        
        for _col in _data1.select_dtypes(exclude=['int', 'int64', 'bool', 'float', 'float64']).columns.tolist():
            _data1[_col] = _data1[_col].map(str)

            _data1[_col] = _data1[_col].apply(
                lambda x: x.encode('ascii', 'ignore').decode('utf-8') if bool(x) else x
            )
            
            _data1[_col] = _data1[_col].apply(
                lambda x: extensions.adapt(x).getquoted().decode('utf-8') if bool(x) else x
            )
            _data1[_col] = _data1[_col].apply(
                lambda x: x.replace("'", "").replace("%", "%%") if bool(x) else x
            )

        table1Values = _data1[
            [
                'id_str',
                'name',
                'screen_name',
                'url',
                'location',
                'followers_count',
                'friends_count',
                'listed_count',
                'favourites_count',
                'statuses_count',
                'created_at',
                'description',
                'lang',
                'verified',
                'profile_image_url_https',
                'profile_background_image_url_https',
                'default_profile',
                'default_profile_image',
            ]
        ]

        table1Values.drop_duplicates(
            'id_str', 
            keep = 'last',
            inplace = True
        )

        preparePushData(
            table1Columns, 
            table1Values, 
            'user_profile'
        )
    except Exception as e:
        print(f'POSTGRES: *** Push for Table -> user_profile Unsuccessful as {e} ***')
    
    try:    
        # Table 2
        table2Columns = [
            'reply_tweet_id',
            'tweet_id'
        ]

        table2Values = _data.loc[
            _data.in_reply_to_status_id_str.notna(),
            [
                'id_str',
                'in_reply_to_status_id_str',

            ]
        ]

        table2Values.drop_duplicates(
            'id_str', 
            keep = 'last',
            inplace = True
        )
        
        preparePushData(
            table2Columns, 
            table2Values, 
            'reply'
        )
    except Exception as e:
        print(f'POSTGRES: *** Push for Table -> reply Unsuccessful as {e} ***')
    
    try:    
        # Table 3
        table3Columns = [
            'quoted_tweet_id',
            'tweet_id'
        ]

        table3Values = _data.loc[
            _data.quoted_status_id_str.notna(),
            [
                'id_str',
                'quoted_status_id_str'
            ]
        ]

        table3Values.drop_duplicates(
            'id_str', 
            keep = 'last',
            inplace = True
        )
        
        preparePushData(
            table3Columns, 
            table3Values, 
            'quoted_tweets'
        )
    except Exception as e:
        print(f'POSTGRES: *** Push for Table -> quoted_tweets Unsuccessful as {e} ***')

    try:    
        # Table 4
        table4Columns = [
            'retweet_id',
            'tweet_id'
        ]

        _data1 = _data[
            _data.retweeted_status.notna()
        ]

        _data1['tweet_id'] = _data1['retweeted_status'].apply(
            lambda x: x["id_str"]
        )
        
        table4Values = _data1[
            [
                'id_str',
                'tweet_id'
            ]
        ]
        
        table4Values.drop_duplicates(
            'id_str', 
            keep = 'last',
            inplace = True
        )

        preparePushData(
            table4Columns, 
            table4Values, 
            'retweets'
        )
    except Exception as e:
        print(f'POSTGRES: *** Push for Table -> retweets Unsuccessful as {e} ***')
    
    try:    
        # Table 5
        table5Columns = [
            'tweet_id',
            'user_id',
            'tweet_flag'
        ]

        table5Values = _data[
            [
                'id_str',
                'user_id',
                'flag'
            ]
        ]

        table5Values.drop_duplicates(
            'id_str', 
            keep = 'last',
            inplace = True
        )
        
        preparePushData(
            table5Columns, 
            table5Values, 
            'tweets'
        )
    except Exception as e:
        print(f'POSTGRES: *** Push for Table -> tweets Unsuccessful as {e} ***')

if __name__ == "__main__":
    conn = connSQL()
    cur = conn.cursor()
    
    # Create POSTGRES tables
    print('POSTGRES: *** Table Creation Started ***')
    createPostgresTables(cur)
    print('POSTGRES: *** All Tables Successfully Created ***')
    
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

    twitterdf['flag'] = ''

    twitterdf['created_at'] = pd.to_datetime(twitterdf['created_at'])
    twitterdf['created_at'] = twitterdf['created_at'].dt.strftime('%a %b %d %H:%M:%S +0000 %Y')

    for index, row in twitterdf.iterrows():
        # Check if quoted_status and retweeted_status are not NaN
        if not pd.isnull(row['quoted_status']) and not pd.isnull(row['retweeted_status']):
            # Extract the created_at dates for the quoted and retweeted tweets
            quoted_created_at = row['quoted_status']['created_at']
            retweeted_created_at = row['retweeted_status']['created_at']
            
            # Compare the dates and set the flag column accordingly
            if quoted_created_at > retweeted_created_at:
                twitterdf.loc[index, 'flag'] = 'quoted_tweet_flag'
            else:
                twitterdf.loc[index, 'flag'] = 'retweet_flag'
        
        if not pd.isnull(row['quoted_status']) and not pd.isnull(row['in_reply_to_user_id_str']):
            # Extract the created_at dates for the quoted and reply tweets
            quoted_created_at = row['quoted_status']['created_at']
            reply_created_at = row['created_at']
            
            # Compare the dates and set the flag column accordingly
            if quoted_created_at > reply_created_at:
                twitterdf.loc[index, 'flag'] = 'quoted_tweet_flag'
            else:
                twitterdf.loc[index, 'flag'] = 'reply_tweet_flag'

        elif not pd.isnull(row['quoted_status']):
            twitterdf.loc[index, 'flag'] = 'quoted_tweet_flag'

        elif not pd.isnull(row['retweeted_status']):
            twitterdf.loc[index, 'flag'] = 'retweet_flag'
            
        elif not pd.isnull(row['in_reply_to_user_id_str']):
            twitterdf.loc[index, 'flag'] = 'reply_tweet_flag'
        else:
            twitterdf.loc[index, 'flag'] = 'original_tweet_flag'

    # twitterdf['retweet_flag'] = np.where(
    #     twitterdf['retweeted_status'].isna(), 
    #     False, 
    #     True
    # )

    # twitterdf['quoted_tweet_flag'] = np.where(
    #     twitterdf['quoted_status_id_str'].isna(), 
    #     False, 
    #     True
    # )

    # twitterdf['reply_tweet_flag'] = np.where(
    #     twitterdf['in_reply_to_user_id_str'].isna(), 
    #     False, 
    #     True
    # )

    # twitterdf['original_tweet_flag'] = np.where(
    #     (twitterdf['retweet_flag'] == False) & 
    #     (twitterdf['quoted_tweet_flag'] == False) & 
    #     (twitterdf['reply_tweet_flag'] == False),
    #     True,
    #     False
    # )
    
    # # Drop duplicates on user_id
    # twitterdf.drop_duplicates(subset = ["user_id"], keep = "last", inplace = True)

    # Reset indices
    twitterdf.reset_index(inplace = True, drop = True)

    # Call push functions
    print('POSTGRES: *** Starting table push ***')
    pushPostgresData(cur, twitterdf) 
    print('POSTGRES: *** Table Push Successful for all tables ***')

    # Commit & close the cursor & connection
    cur.close()
    conn.commit()
    conn.close()
