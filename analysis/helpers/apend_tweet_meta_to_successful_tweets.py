#!/usr/bin/env python
# append_tweet_meta_to_successful_tweets.py

# ANALYSIS (helpers): TWITTER
# we've pulled our top quoted, RTd and replied to tweets.
# we've produced counts for how often these tweets occur in our samples.
# WHAT WE WANT NOW is to...
# append tweet data (text, username, etc) to these tweet dataframes.
# as this is quite a costly task, let's try and do this in a script 
# in the background

# NL, 08/01/23

############
# IMPORTS
############
import os
import json
import pandas as pd
from tqdm import tqdm

############
# PATHS & CONSTANTS
############
TWEETS_PATH = '/home/nikloynes/projects/world_cup_misinfo_tracking/data/tweets_clean/'
EXPORT_PATH = '/home/nikloynes/projects/world_cup_misinfo_tracking/data/exports/tweets/'

QUOTED_TWEETS_PATH = EXPORT_PATH+'top_quoted_tweets.csv'
RETWEETED_TWEETS_PATH = EXPORT_PATH+'top_retweeted_tweets.csv'
REPLIED_TO_TWEETS_PATH = EXPORT_PATH+'top_replied_to_tweets.csv'

############
# INIT
############
quoted_ids = list(pd.read_csv(QUOTED_TWEETS_PATH)['tweet_id'])
retweeted_ids = list(pd.read_csv(QUOTED_TWEETS_PATH)['tweet_id'])
replied_ids = list(pd.read_csv(REPLIED_TO_TWEETS_PATH)['tweet_id'])

quoted = []
retweeted = []
replied_to = []

# tweet files
tweet_files = os.listdir(TWEETS_PATH)
tweet_files.sort()
tweet_files.remove('tweets_2022_12_18-16_00_01-old_broken.json')

############
# THE THING
############
for file in tqdm(tweet_files):
    with open(TWEETS_PATH+file, 'r') as infile:
        tweets = [json.loads(line) for line in infile]

    for tweet in tweets:
        if str(tweet['id']) in quoted_ids:
            quoted.append(tweet)

        if str(tweet['id']) in retweeted_ids:
            retweeted.append(tweet)

        if str(tweet['id']) in replied_ids:
            replied_to.append(tweet)

quoted_match_df = pd.DataFrame(quoted)
quoted_match_df = quoted_match_df.rename(columns={'id' : 'tweet_id'})

retweeted_match_df = pd.DataFrame(retweeted)
retweeted_match_df = retweeted_match_df.rename(columns={'id' : 'tweet_id'})

replied_match_df = pd.DataFrame(replied_to)
replied_match_df = replied_match_df.rename(columns={'id' : 'tweet_id'})

# out
quoted_match_df.to_csv(EXPORT_PATH+'top_quoted_meta_tmp.csv', index=False)
retweeted_match_df.to_csv(EXPORT_PATH+'top_retweeted_meta_tmp.csv', index=False)
replied_match_df.to_csv(EXPORT_PATH+'top_replied_to_meta_tmp.csv', index=False)

# merge with count dfs
quoted_df = pd.read_csv(QUOTED_TWEETS_PATH).astype({'tweet_id' : str})
retweeted_df = pd.read_csv(RETWEETED_TWEETS_PATH).astype({'tweet_id' : str})
replied_to_df = pd.read_csv(REPLIED_TO_TWEETS_PATH).astype({'tweet_id' : str})

quoted_df = quoted_df.merge(quoted_match_df, on='tweet_id', how='left')
retweeted_df = retweeted_df.merge(retweeted_match_df, on='tweet_id', how='left')
replied_to = replied_to_df.merge(replied_match_df, on='tweet_id', how='left')

quoted_df = quoted_df.sort_values('freq', ascending=False)
retweeted_df = retweeted_df.sort_values('freq', ascending=False)
replied_to_df = replied_to_df.sort_values('freq', ascending=False)

# out
quoted_df.to_csv(EXPORT_PATH+'top_quoted_meta.csv', index=False)
retweeted_df.to_csv(EXPORT_PATH+'top_retweeted_meta.csv', index=False)
replied_to_df.to_csv(EXPORT_PATH+'top_replied_to_meta.csv', index=False)