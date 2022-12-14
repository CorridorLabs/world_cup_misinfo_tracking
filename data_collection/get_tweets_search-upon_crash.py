#!/usr/bin/env python
# get_tweets_search.py

# DATA COLLECTION: TWITTER
# pulling tweets and relevant metadata from the twitter api
# via the tweepy module for python.
# TWIST: using the search endpoint method via client

# NOTES:
# - add something that creates the start_time, end_time and filename
#   thing for us. RIGHT NOW, THIS IS BAKED INTO THE CODE.

# NL, 05/12/22

############
# IMPORTS
############
import os
import sys
from dotenv import load_dotenv
import json
import argparse
import re
import datetime
from dateutil import parser as date_parser
from time import sleep
import logging
import tweepy

############
# FUNCTIONS
############
def check_path_exists(filepath:str):
    '''
    checks if a supplied filepath 
    (dir + filename) exists. if the full path
    with file is not a file, checks for 
    just the dir path and creates file if exists, 
    raises error if not
    
    args:
        - filepath: str, full file path
    '''
    if filepath is None:
        raise TypeError(f'specified path is None.')

    if not isinstance(filepath, str):
        raise TypeError(f'filepath object must be\
            str. Please re-specify.')

    if os.path.isdir(filepath):
        raise ValueError(f'Need to provide a full path to a file,\
            not a dir.')

    if not os.path.isfile(filepath):
        # split and check if everything before the last 
        # `/` is a dir
        splits = filepath.split('/')
        concat = '/'.join(splits[:-1])+'/'
        if not os.path.isdir(concat):
            raise NotADirectoryError(f'Directory path \
                {concat} does not exist. Please re-specify\
                    `out_path`.')
        else:
            logging.info(f'{concat}, the dir in for\
                specified filepath is a directory, but\
                    file {splits[-1:]} does not exist.\
                        Thats fine for us.')
            return filepath
    
    else: 
        return filepath


def extract_count_domains_entities(context_field:list):
    '''
    entracts the counts of domains and entities in 
    a given tweet.

    returns:
        - domains, dict
        - entities, dict
    '''
    domains = {}
    entities = {}

    for context in context_field:
        # domain
        if context['domain']['name'] not in domains.keys():
            domains[context['domain']['name']] = 1
        else:
            domains[context['domain']['name']] += 1

        # entity
        if context['entity']['name'] not in entities.keys():
            entities[context['entity']['name']] = 1
        else:
            entities[context['entity']['name']] += 1

    return domains, entities


def total_domain_entity_counts(domains_tweet:dict,
                               domains_session:dict,
                               entities_tweet:dict,
                               entities_session:dict):
    ''' 
    accumulates counts for domains and entities
    for the entire streaming session.  
    '''
    for domain in domains_tweet.keys():
        if domain not in domains_session.keys():
            domains_session[domain] = 1
        else:
            domains_session[domain] += 1

    for entity in entities_tweet.keys():
        if entity not in entities_session.keys():
            entities_session[entity] = 1
        else:
            entities_session[entity] += 1

    return domains_session, entities_session


def extract_urls(tweet_text:str) -> list:
    '''
    extracts urls from tweet text
    returns all urls in tweet text in list
    '''
    urls = re.findall("http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+", tweet_text)

    return urls

############
# CLI 
############
parser = argparse.ArgumentParser(description='Parameters for getting tweets via search method.')

# -a ACTION_TYPE
parser.add_argument("-s", "--search_terms", dest = "search_terms", 
                    default="twitter_search_terms.txt",
                    help="""path to txt file containing search terms
                    to filter the twitter stream by.""")

parser.add_argument("-o", "--out_path", dest = "out_path", 
                    default="../data/tweets/",
                    help="directory to which to write tweets")

parser.add_argument("-m", "--meta_path", dest = "meta_path", 
                    default="../data/tweets_meta/",
                    help="directory to which to write domain/entity metadata")

parser.add_argument("-i", "--iterations", dest = "iterations", 
                    default=1000,
                    help="""number of iterations/pages to run through""")

parser.add_argument("-l", "--log_to_stdout", dest = "log_to_stdout", 
					action = "store_true",
					help= """a flag that indicates whether to print logging 
					messages to stdout as well as file.""")

args = parser.parse_args()

############
# PATHS & CONSTANTS
############
TWEETS_DIR = args.out_path
META_DIR = args.meta_path
ITS = args.iterations

SEARCH_TERMS = args.search_terms

# fields to return in tweets
EXPANSTIONS = ['author_id', 'referenced_tweets.id']
TWEET_FIELDS = ['created_at', 'public_metrics', 'source', 'context_annotations']
MEDIA_FIELDS = ['media_key', 'type', 'url', 'duration_ms']
USER_FIELDS = ['id', 'name', 'username', 'created_at', 'description', 'location', 'public_metrics', 'protected']

# fields we want to retain for our json
OUT_CORE_FIELDS = ['author_id', 'created_at', 'id', 'public_metrics', 'referenced_tweets', 'source', 'text'] 
OUT_USER_FIELDS = ['created_at', 'description', 'id', 'location', 'name', 'public_metrics', 'username']

# check our out dirs exist - create if no
if not os.path.isdir(TWEETS_DIR):
    os.mkdir(TWEETS_DIR)

if not os.path.isdir(META_DIR):
    os.mkdir(META_DIR)

# DATETIME STUFF
DT_TODAY = datetime.datetime.now()
TODAY = DT_TODAY.strftime('%Y_%m_%d-%H_%M_%S')

# outfiles
# time stamps
# we want to generate a list of dicts with 8 entries. for each entry we will have these keys: filename, start_time, end_time
# NOTE: THIS NEEDS TO BE UPDATED WITH A CLI-DERIVED APPROACH.
earliest = date_parser.parse('2022-12-04 18:00:01')
delta = datetime.timedelta(minutes=59)
time_chunks = []

for i in range(6):
    tmp = {
        'start_time' : earliest,
        'end_time' : earliest+delta,
        'tweets_path' : TWEETS_DIR+'tweets_'+earliest.strftime('%Y_%m_%d-%H_%M_%S')+'.json',
        'entities_path' : META_DIR+'entities_'+earliest.strftime('%Y_%m_%d-%H_%M_%S')+'.json',
        'domains_path' : META_DIR+'domains_'+earliest.strftime('%Y_%m_%d-%H_%M_%S')+'.json'
    }

    time_chunks.append(tmp)
    earliest = earliest + datetime.timedelta(hours=1)

time_chunks[0]['end_time'] = time_chunks[0]['end_time']-datetime.timedelta(minutes=35)

# logging
LOG_FILE_PATH = f'../data/logfiles/twitter/twitter_search_{TODAY}.log'
LOG_FORMAT = '%(asctime)s [%(filename)s:%(lineno)s - %(funcName)20s() ] - %(name)s - %(levelname)s - %(message)s'

############
# INIT
############
load_dotenv()
bearer_token = os.getenv('TWITTER_BEARER_TOKEN')

# build query string from search terms
with open(SEARCH_TERMS, 'r') as infile:
    search_terms = "".join(['('+line.rstrip()+')'+' OR ' for line in infile][:5])
# drop the final 'or'
search_terms = search_terms[:-4]

# Logger
file_handler = logging.FileHandler(filename=LOG_FILE_PATH)
stdout_handler = logging.StreamHandler(sys.stdout)

if args.log_to_stdout:
	handlers = [file_handler, stdout_handler]
else: 
	handlers = [file_handler]

logging.basicConfig(
    level=logging.INFO, 
    format=LOG_FORMAT,
    handlers=handlers)

logger = logging.getLogger('LOGGER')

############
# THE THING!
############
# instantiate our client
client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)

# we have a big loop here:
# 1. loop around our chunk
# 2. paginate for 1000 iterations on that chunk
# 3. for each of those iterations, process every single tweet, and write out to file.
# -- while we do this, log our progress. 
n_tweets_total = 0

for chunk in time_chunks:
    logging.info(f'Now collecting tweets from {chunk["start_time"]} to {chunk["end_time"]}.')
    it_counter = 0
    n_tweets_chunk = 0

    chunk_domains = {}
    chunk_entities = {}

    # trying to handle the big TwitterServiceError
    # through wrapping the paginator in a try block, and also sleeping for a second before 
    # getting through new pages. 
    try:
        for tweets in tweepy.Paginator(client.search_recent_tweets, 
                                    query=search_terms,
                                    start_time=chunk['start_time'],
                                    end_time=chunk['end_time'],
                                    expansions=EXPANSTIONS, 
                                    tweet_fields=TWEET_FIELDS,
                                    media_fields=MEDIA_FIELDS,
                                    user_fields=USER_FIELDS,
                                    max_results=100,
                                    limit=ITS):
            it_counter += 1
            logging.info(f'on page {it_counter} out of {ITS}. n tweets collected in chunk so far: {n_tweets_chunk}. n tweets collected total so far: {n_tweets_total}')

            for i in range(len(tweets[0])):
                tmp_dict = tweets[0][i].data
                author_id = tmp_dict['author_id']
                tmp_user = {}
                
                try:
                    tmp_user = tweets[1]['users'][i].data
                    if tmp_user['id']!=author_id:
                        logging.info(f'we have encountered a tweet-user mismatch in tweet {tmp_dict["id"]}. looking for correct user id')
                        # we now need to find the correct user object in there
                        for user_obj in tweets[1]['users']:
                            if user_obj.data['id']==author_id:
                                logging.info(f'found correct user object for tweet {tmp_dict["id"]}.')
                                tmp_user = user_obj.data
                except IndexError:
                    for user_obj in tweets[1]['users']:
                        if user_obj.data['id']==author_id:
                            logging.info(f'found correct user object for tweet {tmp_dict["id"]}.')
                            tmp_user = user_obj.data
                
                if 'id' in tmp_user.keys():
                    if tmp_user['id']!=author_id:
                        tmp_user = {'not available'}
                        logging.info(f'unable to find correct user object for tweet {tmp_dict["id"]}.')

                out = {}
                for field in OUT_CORE_FIELDS:
                    if field in tmp_dict:
                        out[field] = tmp_dict[field]

                urls = extract_urls(tmp_dict['text'])
                if len(urls)>0:
                    out['urls'] = urls

                # extract domain and entity counts
                if 'context_annotations' in tmp_dict.keys():
                    logging.info(f'extracting domain and entity counts for tweet {tmp_dict["id"]}')
                    domains, entities = extract_count_domains_entities(tmp_dict['context_annotations'])
                    out['domains'] = domains
                    out['entities'] = entities

                    chunk_domains, chunk_entities = total_domain_entity_counts(domains_tweet=domains,
                                                                            domains_session=chunk_domains,
                                                                            entities_tweet=entities,
                                                                            entities_session=chunk_entities)

                # add user stuff
                user = {}
                if tmp_user!={'not available'}:
                    for field in OUT_USER_FIELDS:
                        if field in tmp_user:
                            user[field] = tmp_user[field]

                    out['user'] = user
                else:
                    out['user'] = tmp_user

                with open(chunk['tweets_path'], 'a') as o:
                    o.write(json.dumps(out)+'\n')

                n_tweets_chunk += 1
                n_tweets_total += 1

                # adding sleep statement to avoid server errors 
            sleep(1)

    except tweepy.TwitterServerError as e:
        logging.info(f'Encountered Twitter Server error; message: {e}. Sleeping for 300 seconds.')
        sleep(300)

    logging.info(f'Completed pulling tweets for chunk starting {chunk["start_time"]}')

    logging.info(f'Now writing out domain and entity counts for the chunk of tweets starting {chunk["start_time"]}')
    chunk_domains = dict(sorted(chunk_domains.items(), key=lambda x:x[1], reverse=True))
    chunk_entities = dict(sorted(chunk_entities.items(), key=lambda x:x[1], reverse=True))

    with open(chunk['domains_path'], 'w') as o:
        o.write(json.dumps(chunk_domains))

    with open(chunk['entities_path'], 'w') as o:
        o.write(json.dumps(chunk_entities))

logging.info(f'Completed data collection from twitter search. Total tweets collected {n_tweets_total}.')