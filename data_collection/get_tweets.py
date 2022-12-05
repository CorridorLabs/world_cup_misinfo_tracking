#!/usr/bin/env python
# get_tweets.py

# DATA COLLECTION: TWITTER
# STREAMING tweets and relevant metadata from the twitter api
# via the tweepy module for python.

# logic/sequence:
# - TO FILL IN

# NL, 26/11/22

############
# IMPORTS
############
import os
import sys
from dotenv import load_dotenv
import argparse
import json
import logging
import re
import datetime
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
parser = argparse.ArgumentParser(description='Parameters for getting tweets.')

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
                    help="directory to which to write tweets")

parser.add_argument("-k", "--kill_time", dest = "kill_time", 
                    default=59,
                    help="""time after which streaming process is
                    to be killed""")

parser.add_argument("-t", "--time_unit", dest = "time_unit",
                    default='minutes',
                    help="""measurement of time for the kill-time
                    parameter - either `minutes` or `seconds`""")

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
KILL_TIME = args.kill_time

SEARCH_TERMS = args.search_terms

# fields to return in tweets
EXPANSTIONS = ['author_id', 'referenced_tweets.id']
TWEET_FIELDS = ['created_at', 'public_metrics', 'source', 'context_annotations']
MEDIA_FIELDS = ['media_key', 'type', 'url', 'duration_ms']
USER_FIELDS = ['id', 'name', 'username', 'created_at', 'description', 'location', 'public_metrics']

# check our out dirs exist - create if no
if not os.path.isdir(TWEETS_DIR):
    os.mkdir(TWEETS_DIR)

if not os.path.isdir(META_DIR):
    os.mkdir(META_DIR)

# DATETIME STUFF
DT_TODAY = datetime.datetime.now()
TODAY = DT_TODAY.strftime('%Y_%m_%d-%H_%M_%S')
OUTFILE = TWEETS_DIR+'tweets_'+TODAY+'.json'
DOMAINS_FILE = META_DIR+'domains_'+TODAY+'.json'
ENTITIES_FILE = META_DIR+'entities_'+TODAY+'.json'

# logging
LOG_FILE_PATH = f'../data/logfiles/twitter/{TODAY}.log'
LOG_FORMAT = '%(asctime)s [%(filename)s:%(lineno)s - %(funcName)20s() ] - %(name)s - %(levelname)s - %(message)s'

############
# INIT
############
load_dotenv()
bearer_token = os.getenv('TWITTER_BEARER_TOKEN')

# pull in search terms
with open(SEARCH_TERMS, 'r') as infile:
    search_terms = [line.rstrip() for line in infile]

# Logger
file_handler = logging.FileHandler(filename=LOG_FILE_PATH)
stdout_handler = logging.StreamHandler(sys.stdout)

if args.log_to_stdout:
	handlers = [file_handler, stdout_handler]
else: 
	handlers = [file_handler]

logging.basicConfig(
    level=logging.DEBUG, 
    format=LOG_FORMAT,
    handlers=handlers)

logger = logging.getLogger('LOGGER')

############
# THE THING!
############
# first, our streamer class
class TweetStreamer(tweepy.StreamingClient):

    def __init__(self, 
                 out_path:str,
                 out_path_domains:str,
                 out_path_entities:str,
                 kill_time:int=59,
                 time_unit:str='minutes',
                 **kwargs):
        '''
        adding custom params
        '''
        # out path for our tweet json
        out_path = check_path_exists(out_path)
        self.outfile = out_path 

        # out path for our domains/entities jsons
        out_path_domains = check_path_exists(out_path_domains)
        out_path_entities = check_path_exists(out_path_entities)
        self.out_path_domains = out_path_domains
        self.out_path_entities = out_path_entities

        # timing stuff
        self.start_time = datetime.datetime.now()

        if time_unit not in ['seconds', 'minutes']:
            raise ValueError(f'time_unit must be either `minutes` or `seconds`.')

        if time_unit=='minutes':
            self.kill_time = datetime.timedelta(seconds=60*kill_time)
        else:
            self.kill_time = datetime.timedelta(seconds=kill_time)

        self.domains = {}
        self.entities = {}

        self.counter = 0

        # using super here makes sure we get all the attributes
        # from our super-class. we do have to pass **kwargs both in
        # the init method and here for this to work.
        super(TweetStreamer, self).__init__(**kwargs)
    
    def on_data(self, data):
        '''
        1. clean the returned tweet object
        2. write it out
        '''
        # pull core fields we want
        if (datetime.datetime.now() - self.start_time) <= self.kill_time:
            self.counter += 1
            obj = json.loads(data)
            tweet = obj['data']
            del tweet['edit_history_tweet_ids']

            urls = extract_urls(tweet['text'])
            if len(urls)>0:
                tweet['urls'] = urls

            if 'context_annotations' in tweet.keys():
                domains, entities = extract_count_domains_entities(tweet['context_annotations'])
                self.domains, self.entities = total_domain_entity_counts(domains_tweet=domains,
                                                                         domains_session=self.domains,
                                                                         entities_tweet=entities,
                                                                         entities_session=self.entities)
                
                # for domain in domains.keys():
                #     if domain not in self.domains.keys():
                #         self.domains[domain] = 1
                #     else:
                #         self.domains[domain] += 1

                # for entity in entities.keys():
                #     if entity not in self.entities.keys():
                #         self.entities[entity] = 1
                #     else:
                #         self.entities[entity] += 1
                
                del tweet['context_annotations']
                tweet['domains'] = domains
                tweet['entities'] = entities

            tweet['user'] = obj['includes']['users'][0]
            
            with open(self.outfile, 'a') as o:
                o.write(json.dumps(tweet)+'\n')

        # encountered the time limit
        else:
            # write out our domain and entity counts
            self.domains = dict(sorted(self.domains.items(), key=lambda x:x[1], reverse=True))
            self.entities = dict(sorted(self.entities.items(), key=lambda x:x[1], reverse=True))

            with open(self.out_path_domains, 'w') as o:
                o.write(json.dumps(self.domains))

            with open(self.out_path_entities, 'w') as o:
                o.write(json.dumps(self.entities))

            # number of tweets collected to log file
            logging.info(f'Collected a total of {self.counter} tweets.')
            
            # this kills the streaming process
            self.disconnect()
            return False


    def on_errors(self, errors):
        return super().on_errors(errors)


# instantiate the class
streamer = TweetStreamer(bearer_token=bearer_token, 
                         out_path=OUTFILE, 
                         out_path_domains=DOMAINS_FILE, 
                         out_path_entities=ENTITIES_FILE,
                         kill_time=int(KILL_TIME),
                         time_unit=args.time_unit)

# # delte existing streaming rules
existing_rules = streamer.get_rules()
logging.info(f'Current rules: {existing_rules}')
streamer.delete_rules([x.id for x in existing_rules.data])

# # add our target rules
for item in search_terms:
    try:
        streamer.add_rules(tweepy.StreamRule(item))
    except tweepy.HTTPException:
        break

# start filtering
streamer.filter(
    # backfill_minutes=1,
    expansions=EXPANSTIONS, 
    tweet_fields=TWEET_FIELDS,
    media_fields=MEDIA_FIELDS,
    user_fields=USER_FIELDS
)