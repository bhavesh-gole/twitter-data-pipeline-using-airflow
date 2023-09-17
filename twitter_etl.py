import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

def run_twitter_etl():
    access_key = "hkceCkDOWzNEzHjNSxUWNjmWX"
    access_secret = "XJu9rYBa81JvGD19kWDS7qszWtIexOFLICtGb2lg6Vk8MpKDXx"
    consumer_key = "1675029726295883776-U4Vh8cCmC5tvShOJut7wyjoCX3sL8i"
    consumer_secret = "Anv59lViCBcsPxX7zkbTfHnpa2LrcW6OKvKfeyBrd4gfo"

    #twitter auth
    auth = tweepy.OAuth1UserHandler(access_key,access_secret)
    auth.set_access_token(consumer_key,consumer_secret)

    #creating API object
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name ='@Xbox',
                            #200 is the maximum allowd count
                            count = 200,
                            include_rts = False,
                            #imp to keep full text
                            tweet_mode ='extended')

    tweet_list = []
    for tweets in tweets:
        text = tweet._json['full_text']
        
        redefined_tweet = {"user":tweet.user.screen_name,
                        'text' : text,
                        'favourite_count':tweet.favourite_count,
                        'retweet_count':tweet.retweet_count,
                        'created_at':tweet.created_at}
        
        tweet_list.append(redefined_tweet)
        
        df = pd.DataFrame(tweet_list)
        df.to_csv('s3://bhavesh-airflow/Xbox_twitter_data.csv')