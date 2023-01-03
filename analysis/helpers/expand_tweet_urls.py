#!/usr/bin/env python
# expand_tweet_urls.py

# ANALYSIS (helpers): TWITTER
# batch-expanding all unique links contained in our collected tweets.

# NL, 03/01/23

############
# IMPORTS
############
import json
import urlexpander
import pandas as pd

############
# PATHS & CONSTANTS
############
URLS_PATH = '/home/nikloynes/projects/world_cup_misinfo_tracking/data/exports/tweets/urls_counts.json'
CACHE_PATH = '/home/nikloynes/projects/world_cup_misinfo_tracking/data/exports/tweets/tmp_expanded.json'
OUT_PATH = '/home/nikloynes/projects/world_cup_misinfo_tracking/data/exports/tweets/expanded.csv'

############
# THE THING!
############
# read in our urls
with open(URLS_PATH, 'r') as infile:
    urls = json.load(infile)

out_df = pd.DataFrame.from_dict(urls, 
                       orient='index').reset_index().rename(columns={'index' : 'url', 0 : 'count'})

resolved_links = urlexpander.expand(out_df['url'], 
                                    chunksize=1280,
                                    n_workers=64, 
                                    cache_file=CACHE_PATH, 
                                    verbose=1)

out_df['expanded'] = resolved_links

out_df.to_csv(OUT_PATH, index=False)