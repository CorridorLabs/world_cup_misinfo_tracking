#!/usr/bin/env python
# get_reddit_posts.py

# DATA COLLECTION: REDDIT
# pulling posts and relevant metadata from reddit using the reddit api
# via the PRAW module in Python

# logic/sequence:
# - initialise everything - api connection, subreddits to pull, etc.
# - pull the most recent timestamp for an existing subreddit collection, 
#   if it exists
# - start pulling data with our established functions. drop a post if it's older
#   than our most recently collected post.

# NL, 29/11/22

############
# IMPORTS
############
import os
import sys
import argparse
from dotenv import load_dotenv
import json
import logging
import datetime as dt
from dateutil import parser as date_parser
import praw
from prawcore.exceptions import NotFound
from time import sleep

############
# FUNCTIONS
############
def get_user_attribute(user:praw.models.reddit.redditor.Redditor,
                       attribute:str):
    '''  
    helper function for getting user-level attributes from a praw
    redditor object. this function avoids breaking code due to 
    missing attributes when throwing an attribute error
    '''
    try:
        val = getattr(user, attribute, None)
    except NotFound:
        val = None
        
    return val


def reddit_post_to_dict(post:praw.models.reddit.submission.Submission,
                        custom_fields:list=None,
                        overwrite_core_fields:bool=False,
                        convert_timestamp:bool=True) -> dict:
    '''
    function that converts an object of the praw.submission type
    into a dict. this is useful primarily in order to serialise this
    to json and write out to file.

    args:
        - post, a submission ('post') in a subreddit retrieved via praw
        - custom_fields, a list of strings containing additional fields returned by 
          praw which are to be retained
        - overwrite_core_fields, bool, indicates whether core (standard) fields
          of the output dict are to be retained, or whether they should be replaced
          entirely by 'custom_fields'
    '''    
    if not isinstance(post, praw.models.reddit.submission.Submission):
        raise TypeError('post must be a praw submission object')

    if overwrite_core_fields:
        if not custom_fields:
            raise ValueError('custom_fields is not defined')

    core_fields = ['id', 'created_utc', 'title', 'selftext', 'domain', 'url', 'num_comments', 'score', 'ups', 'downs', 'author']
    if overwrite_core_fields:
        core_fields = custom_fields
    elif isinstance(custom_fields, list) and len(custom_fields)>0:
        core_fields += custom_fields

    core_user_fields = ['name', 'id', 'total_karma', 'verified', 'created_utc']

    tmp_dict = vars(post)
    out = {field:tmp_dict[field] for field in core_fields if field in tmp_dict.keys()}

    if 'created_utc' in out.keys():
        if convert_timestamp:
            out['created_utc'] = dt.datetime.fromtimestamp(out['created_utc']).strftime('%Y/%m/%d %H:%M:%S')

    if 'author' in out.keys():
        # we will now process the author object and append to our out object
        user_dict = {'user_'+field:get_user_attribute(user=post.author, attribute=field) for field in core_user_fields}
        
        del out['author']

        out.update(user_dict)

        if convert_timestamp:
            if 'user_created_utc' in out.keys() and isinstance(out['user_created_utc'], float):
                out['user_created_utc'] = dt.datetime.fromtimestamp(out['user_created_utc']).strftime('%Y/%m/%d %H:%M:%S')

    return out


def extract_newest_date_from_json(reddit_post_json:str) -> dt.datetime:
    ''' 
    when we re-start collecting reddit post data, 
    we want to make sure we don't collect any posts we've already
    retrieved. so, we'll get the date/time of when the most recently
    collected post was published.

    args:  
        - reddit_post_json: full filepath to the json we want to 
          extract a date from.  
    '''
    if not os.path.isfile(reddit_post_json):
        raise ValueError(f'{reddit_post_json} is not a valid file. Please supply a valid reddit json file')

    with open(reddit_post_json, 'r') as infile:
        for line in infile:
            tmp = json.loads(line)
            break

    date = date_parser.parse(tmp['created_utc'])

    return date


############
# CLI 
############
parser = argparse.ArgumentParser(description='Parameters for pulling reddit posts.')

# -a ACTION_TYPE
parser.add_argument("-s", "--subreddits", dest = "subreddits", 
                    default="reddit_subreddits.txt",
                    help="""path to txt file containing names of
                    subreddits to track.""")

parser.add_argument("-o", "--out_path", dest = "out_path", 
                    default="../data/reddit_posts/",
                    help="directory to which to write reddit posts")

parser.add_argument("-l", "--log_to_stdout", dest = "log_to_stdout", 
					action = "store_true",
					help= """a flag that indicates whether to print logging 
					messages to stdout as well as file.""")

args = parser.parse_args()

############
# PATHS & CONSTANTS
############
MIN_DATE = date_parser.parse('2022-11-15')

SUBREDDITS = args.subreddits
OUT_PATH = args.out_path

# DATETIME STUFF
DT_TODAY = dt.datetime.now()
TODAY = DT_TODAY.strftime('%Y_%m_%d-%H_%M_%S')

# logging
LOG_FILE_PATH = f'../data/logfiles/reddit/posts_{TODAY}.log'
LOG_FORMAT = '%(asctime)s [%(filename)s:%(lineno)s - %(funcName)20s() ] - %(name)s - %(levelname)s - %(message)s'

############
# INIT
############
load_dotenv()

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

# pull in search terms
with open(SUBREDDITS, 'r') as infile:
    subreddits = [line.rstrip() for line in infile]

# let's create out-dirs for each of our subreddits to track,
# and record n files avail, as well as most recent date, if avail
sub_out_paths = {}
sub_newest_posts = {}

for sub in subreddits:
    if not os.path.isdir(OUT_PATH+sub+'/'):
        os.mkdir(OUT_PATH+sub+'/')
        logging.info(f'created sub-level out-path: {OUT_PATH+sub}/')
        sub_newest_posts.update({sub : MIN_DATE})
    else:
        logging.info(f'sub-level out-dir {OUT_PATH+sub} already exists.')
        # now getting our cutoff dates for newly incoming posts
        already_collected = os.listdir(OUT_PATH+sub+'/')
        if len(already_collected)>0:
            already_collected.sort(key=lambda x: os.path.getmtime(OUT_PATH+sub+'/'+x))
            cutoff = extract_newest_date_from_json(OUT_PATH+sub+'/'+already_collected[-1])
        else:
            cutoff = MIN_DATE
        sub_newest_posts.update({sub : cutoff})
        logging.info(f'cutoff date for newly incoming posts in {sub}: {cutoff}')
        logging.info(f'number of files in {OUT_PATH+sub}: {len(already_collected)}.')
    sub_out_paths.update({sub : OUT_PATH+sub+'/'})

############
# THE THING!
############
# let's start by instantiating our reddit instance
reddit = praw.Reddit(
    client_id=os.getenv('REDDIT_ID'),
    client_secret=os.getenv('REDDIT_SECRET'),
    user_agent=os.getenv('REDDIT_USER_AGENT'),
    username=os.getenv('REDDIT_USERNAME'),
    password=os.getenv('REDDIT_PASSWORD')
)

# just some logging for potential debugging... 
logging.info('sub_out_paths:')
logging.info(sub_out_paths)

logging.info('sub_date_cutoffs:')
logging.info(sub_newest_posts)

for sub in subreddits:
    # start by creating the cursor
    logging.info(f'now collecting posts in subreddit {sub}')
    res = reddit.subreddit(sub).new(limit=1000)

    sub_counter = 0
    outfile = sub_out_paths[sub]+sub+'_'+TODAY+'.json'
    for post in res:
        # first check our time-cutoff:
        if dt.datetime.fromtimestamp(post.created) < sub_newest_posts[sub]:
            logging.info(f'no new posts retrievable from {sub}. moving on to next sub.')
            break
        else:
            tmp = reddit_post_to_dict(post)
            with open(outfile, 'a') as o:
                o.write(json.dumps(tmp)+'\n')
            sub_counter += 1

    logging.info(f'collected a total of {sub_counter} posts for {sub}.')
    logging.info(f'Now sleeping before moving onto next sub.')
    sleep(30)

