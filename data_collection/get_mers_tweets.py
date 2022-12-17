#!/usr/bin/env python
# get_mers_tweets.py

# DATA COLLECTION: TWITTER
# as part of our project, we're keen to get all 'mers'-related 
# tweets for the week starting 10/12/22. 

# we will achieve this by wrapping a bunch of shell-commands
# called through os.system in order to loop over our
# `get_tweets_search.py` script with our target params.

# NL, 17/12/22

############
# IMPORTS
############
import os
from dateutil import parser as date_parser
import datetime

############
# PATHS & CONSTANTS
############
TWEETS_DIR = '/home/nikloynes/projects/world_cup_misinfo_tracking/data/mers_tweets/'
META_DIR = '/home/nikloynes/projects/world_cup_misinfo_tracking/data/mers_tweets/meta/'
SEARCH_TERMS = '/home/nikloynes/projects/world_cup_misinfo_tracking/data_collection/mers_search_terms.txt'
N_DAYS = 8
START_DATE = '2022-12-10'
DT_START_DATE = date_parser.parse(START_DATE) 
START_TIME = '18:00'
DELTA = 2
DELTA_UNIT = 'hours'

# dict of chunks
# we are going to use the abbreviated cli flags for the 
# script get_tweets_search.py here.

time_chunks = []
for i in range(N_DAYS):
    # format our date
    tmp_date = (DT_START_DATE+datetime.timedelta(i)).strftime('%Y_%m_%d')
    tmp_date2 = (DT_START_DATE+datetime.timedelta(i)).strftime('%Y-%m-%d')

    tmp = {
        '-d' : tmp_date2,
        '-t' : START_TIME,
        '-e' : DELTA,
        '-u' : DELTA_UNIT,
        '-s' : SEARCH_TERMS,
        '-o' : f'{TWEETS_DIR}mers_tweets_{tmp_date}.json',
        '-m' : META_DIR
    }

    if date_parser.parse(f'{tmp_date2}-{START_TIME}') > datetime.datetime.now():
        tmp['-d'] = datetime.datetime.now().strftime('%Y-%m-%d')
        tmp['-t'] = (datetime.datetime.now()-datetime.timedelta(hours=2)).strftime('%H:%M')
    
    time_chunks.append(tmp)

for day in time_chunks:
    cmd = f"""python3 get_tweets_search.py 
    -d {day["-d"]} 
    -t {day["-t"]} 
    -e {day["-e"]} 
    -u {day["-u"]}
    -s {day["-s"]}
    -o {day["-o"]}
    -m {day["-m"]}
    """
    cmd = cmd.replace('\n', '').replace('    ', ' ')

    res = os.system(cmd)
    if res != 0:
        raise ValueError(f'There was an error while running get_mers_tweets.py - check logfiles.')