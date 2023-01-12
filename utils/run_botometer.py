#!/usr/bin/env python
# run_botometer.py

# UTILS: TWITTER.
# This script leverages the `botometer` package, which will give 
# us some really nice insights into whether a given Twitter account might
# not be representing a human.

# REQUIRED:
# - RapidAPI acc, with subscription to Botometer
# - Twitter API credentials 

# NL, 12/01/23

############
# IMPORTS
############
import os
from dotenv import load_dotenv
import argparse
import json
import pandas as pd
import random
import botometer 

############
# CLI 
############
parser = argparse.ArgumentParser(description='Parameters for assessing bot-ness of tweets.')

# -a ACTION_TYPE
parser.add_argument("-i", "--infile", dest = "infile",
                    help="""path to a twitter csv or json file 
                    containing ids of twitter users.""")

parser.add_argument('-o', '--outfile', dest='outfile',
                    help='''full path to the destination file. for now
                    files will be written out to json.''')

parser.add_argument("-f", "--file_type", dest = "file_type", 
                    choices=["csv", "json"],
                    help="filetype of the input twitter file")

parser.add_argument("-u", "--user_id_field", dest = "user_id_field", 
                    default="author_id",
                    help="name of the field/column indicating our user_id field")

parser.add_argument("-m", "--max_iterations", dest = "max_iterations",
                    default=100,
                    help="""maximum number of runs/users to classify bot-ness
                    for.""")

parser.add_argument("-r", "--random_sample", dest = "random_sample",
                    action='store_true',
                    help="""a flag indicating whether to produce a random
                    sample of twitter users rather than sequentially running through
                    all available user_ids (up to our max iterations).""")

parser.add_argument("-l", "--log_to_stdout", dest = "log_to_stdout", 
					action = "store_true",
					help= """a flag that indicates whether to print logging 
					messages to stdout as well as file.""")

args = parser.parse_args()

############
# INIT 
############
# creds & auth
load_dotenv()
BOTOMETER_TOKEN = os.getenv('BOTOMETER_TOKEN')
TWITTER_APP_AUTH = {
    'consumer_key' : os.getenv('TWITTER_API_KEY'),
    'consumer_secret' : os.getenv('TWITTER_API_KEY_SECRET'),
    'access_token' : os.getenv('TWITTER_ACCESS_TOKEN'),
    'access_token_secret' : os.getenv('TWITTER_ACCESS_TOKEN_SECRET')
}  

bom = botometer.Botometer(wait_on_ratelimit=True,
                          rapidapi_key=BOTOMETER_TOKEN,
                          **TWITTER_APP_AUTH)

# let's get our file read in to memory, and build a list of user-ids
print(args.infile)

if args.file_type=='csv':
    tmp_df = pd.read_csv(args.infile)
    user_list = list(set(tmp_df[args.user_id_field]))
elif args.file_type=='json':
    with open(args.infile, 'r') as infile:
        user_list = [json.loads(line)[args.user_id_field] for line in infile]

# random sample? 
if args.random_sample:
    user_list = random.sample(user_list, int(args.max_iterations))
else:
    user_list = user_list[:args.max_iterations]

############
# THE THING 
############
out = []
for result in bom.check_accounts_in(user_list):
    out.append(result)

with open(args.outfile, 'w') as outfile:
    json.dump(out, outfile)

# print(user_list)
# tmp = bom.check_account(user_list[0])
# print(type(tmp))
# print(tmp)