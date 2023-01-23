#!/usr/bin/env python
# expand_mers_urls.py

# ANALYSIS (helpers): MERS-TWITTER
# batch-expanding all unique links contained in our collected mers tweets.

# NL, 03/01/23
# NL, 13/01/23 -- adapting to run this with our mers tweets.
# NL, 23/01/23 -- adapting for EPL tweets.

############
# IMPORTS
############
import urlexpander
import pandas as pd

############
# PATHS & CONSTANTS
############
URLS_PATH = '/home/nikloynes/projects/world_cup_misinfo_tracking/data/exports/epl_tweets/unique_urls_freqs.csv'
CACHE_PATH = '/home/nikloynes/projects/world_cup_misinfo_tracking/data/exports/epl_tweets/tmp_expanded.json'
OUT_PATH = '/home/nikloynes/projects/world_cup_misinfo_tracking/data/exports/epl_tweets/expanded.csv'

############
# THE THING!
############
# read in our urls
unique_urls_freqs_df = pd.read_csv(URLS_PATH)

resolved_links = urlexpander.expand(unique_urls_freqs_df['url'], 
                                    chunksize=1280,
                                    n_workers=64, 
                                    cache_file=CACHE_PATH, 
                                    verbose=1)

unique_urls_freqs_df['expanded'] = resolved_links

unique_urls_freqs_df.to_csv(OUT_PATH, index=False)