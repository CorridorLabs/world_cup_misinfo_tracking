#!/usr/bin/env python
# get_final_tweets.py

# DATA COLLECTION: TWITTER
# pulling all the relevant tweets for the world cup final 
# on 18/12/22 -- retroactively as there was an issue with 
# the server during target collection

# we will achieve this by wrapping a bunch of shell-commands
# called through os.system in order to loop over our
# `get_tweets_search.py` script with our target params.

# NL, 19/12/22 -- adapting from `get_mers_tweets.py` -- abandoned for now

############
# IMPORTS
############
import os
from dateutil import parser as date_parser
import datetime

############
# PATHS & CONSTANTS
############
TWEETS_DIR = '/home/nikloynes/projects/world_cup_misinfo_tracking/data/tweets/'
META_DIR = '/home/nikloynes/projects/world_cup_misinfo_tracking/data/tweets_meta/'
SEARCH_TERMS = '/home/nikloynes/projects/world_cup_misinfo_tracking/data_collection/twitter_search_terms.txt'
N_ITS = 8
START_DATE = '2022-12-18'
DT_START_DATE = date_parser.parse(START_DATE) 
START_TIME = '14:00'
DELTA = 60
DELTA_UNIT = 'hours'

# dict of chunks
# we are going to use the abbreviated cli flags for the 
# script get_tweets_search.py here.

time_chunks = []
for i in range(N_ITS):
    # format our delta
    if DELTA_UNIT=='hours':
        delta = datetime.timedelta(hours=DELTA)
    elif DELTA_UNIT=='minutes':
        delta = datetime.timedelta(minutes=DELTA)
    elif DELTA_UNIT=='seconds':
        delta = datetime.timedelta(seconds=DELTA)

    # format our date
    tmp_date = (DT_START_DATE+delta).strftime('%Y_%m_%d')
    tmp_date2 = (DT_START_DATE+delta).strftime('%Y-%m-%d')

    tmp = {
        '-d' : tmp_date2,
        '-t' : START_TIME,
        '-e' : DELTA,
        '-u' : DELTA_UNIT,
        '-s' : SEARCH_TERMS,
        '-o' : f'{TWEETS_DIR}tweets_{tmp_date}-.json',
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