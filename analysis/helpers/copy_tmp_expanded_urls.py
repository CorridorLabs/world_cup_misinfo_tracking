#!/usr/bin/env python
# copy_tmp_expanded_ulrs.py

# ANALYSIS (helpers): TWITTER
# we're batch-expanding our URLs, which are being stored in a tmp json.
# in order to avoid losing this file (which contains more info than the resulting file
# from our py script, let's make sure it gets copied every so often)
# this is to be run from crontab.

# NL, 04/01/23

############
# IMPORTS
############
import os

############
# PATHS & CONSTANTS
############
INFILE = '/home/nikloynes/projects/world_cup_misinfo_tracking/data/exports/tweets/tmp_expanded.json'
OUTFILE = '/home/nikloynes/projects/world_cup_misinfo_tracking/data/exports/tweets/expanded_urls.json'

############
# THE THING!
############
cmd = f'cp {INFILE} {OUTFILE}'
res = os.system(cmd)
if res !=0:
    raise ValueError(f'`Copying {INFILE} to {OUTFILE} produced an error.')