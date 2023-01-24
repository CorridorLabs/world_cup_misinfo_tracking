#!/usr/bin/env python
# get_tweets_by_id.py

# DATA COLLECTION: TWITTER
# pulling tweets where tweet ids are known

# NL, 24/01/23

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
parser = argparse.ArgumentParser(description='Parameters for getting tweets by ids.')

# -a ACTION_TYPE
parser.add_argument("-i", "--ids_file", dest="ids_file",
                    help="""a text file containing twitter ids, 
                    separated by newlines""")

parser.add_argument("-o", "--out_file", dest = "out_file",
                    help="filepath to which to write tweets")

parser.add_argument("-m", "--meta_path", dest = "meta_path",
                    help="directory to which to write domain/entity metadata")

parser.add_argument("-l", "--log_to_stdout", dest = "log_to_stdout", 
					action = "store_true",
					help= """a flag that indicates whether to print logging 
					messages to stdout as well as file.""")

args = parser.parse_args()

############
# PATHS & CONSTANTS
############
TWEET_IDS_PATH = args.ids_file

TWEETS_PATH = args.out_file
META_PATH = args.meta_path

# fields to return in tweets
EXPANSTIONS = ['author_id', 'referenced_tweets.id']
TWEET_FIELDS = ['created_at', 'public_metrics', 'source', 'context_annotations']
MEDIA_FIELDS = ['media_key', 'type', 'url', 'duration_ms']
USER_FIELDS = ['id', 'name', 'username', 'created_at', 'description', 'location', 'public_metrics', 'protected']

# fields we want to retain for our json
OUT_CORE_FIELDS = ['author_id', 'created_at', 'id', 'public_metrics', 'referenced_tweets', 'source', 'text'] 
OUT_USER_FIELDS = ['created_at', 'description', 'id', 'location', 'name', 'public_metrics', 'username']

# check our out dirs exist - create if no
TWEETS_PATH = check_path_exists(TWEETS_PATH)
if not os.path.isdir(META_PATH):
    os.mkdir(META_PATH)
if META_PATH[-1:]!='/':
    META_PATH += '/'

# DATETIME STUFF
DT_TODAY = datetime.datetime.now()
TODAY = DT_TODAY.strftime('%Y_%m_%d-%H_%M_%S')

# meta filepaths
ENTITIES_PATH = META_PATH+'entities.json'
DOMAINS_PATH = META_PATH+'domains.json'

# logging
LOG_FILE_PATH = f'../data/logfiles/twitter/twitter_get_tweets_by_id_{TODAY}.log'
LOG_FORMAT = '%(asctime)s [%(filename)s:%(lineno)s - %(funcName)20s() ] - %(name)s - %(levelname)s - %(message)s'

############
# INIT
############
load_dotenv()
bearer_token = os.getenv('TWITTER_BEARER_TOKEN')

# build our list of tweet ids
with open(TWEET_IDS_PATH, 'r') as infile:
    # this removes trailing '\n'
    target_ids = [x[:-1] for x in infile]
    
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

# let's split our input list into sublists of len=100 
logging.info(f'Chunking our list of tweet ids into sublists of length 100')
target_ids = test = [target_ids[x:x+100] for x in range(0, len(target_ids), 100)]
logging.info(f'Completed chunking, left with {len(target_ids)} sublists.')

# let's now iterate over our chunked list...
chunk_domains = {}
chunk_entities = {}

for sublist in target_ids:
    tmp = client.get_tweets(ids=sublist,
                            expansions=EXPANSTIONS,
                            tweet_fields=TWEET_FIELDS,
                            media_fields=MEDIA_FIELDS,
                            user_fields=USER_FIELDS)

    for i in range(len(tmp[0])):
        tmp_dict = tmp[0][i].data
        author_id = tmp_dict['author_id']
        tmp_user = {}
        
        try:
            tmp_user = tmp[1]['users'][i].data
            if tmp_user['id']!=author_id:
                logging.info(f'we have encountered a tweet-user mismatch in tweet {tmp_dict["id"]}. looking for correct user id')
                # we now need to find the correct user object in there
                for user_obj in tmp[1]['users']:
                    if user_obj.data['id']==author_id:
                        logging.info(f'found correct user object for tweet {tmp_dict["id"]}.')
                        tmp_user = user_obj.data
        except IndexError:
            for user_obj in tmp[1]['users']:
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

        with open(TWEETS_PATH, 'a') as o:
            o.write(json.dumps(out)+'\n')