#!/usr/bin/env python3
"""
create-collection.py: create a Solr collection instance
"""

import json
import re
import argparse
import sys
import os.path as path
import pandas as pd
import pandas.api.types as pd_types
import numpy as np

from app.solr import create_collection, delete_collection, ping_collection, wait_for_success

DEBUG = True
FILENAME = 'output/node.jsonl'
FILENAME = 'storage/AA_003.jsonl'
FILENAME = 'storage/HD_001.jsonl'
FILENAME = 'nested.jsonl'
FILENAME = 'storage/BS_004.jsonl'
FILENAME = 'nested2.jsonl'
FILENAME = 'nested3.jsonl'
FILENAME = 'nested4.jsonl'
FILENAME = 'output/block.jsonl'

CORE = None
STRFIELDS = None
INTFIELDS = None
NONUMERIC = False
RENAMEID = False
BITMAP = True
SEQ = None

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description='Create Solr collection based on filename')
    PARSER.add_argument('inputfiles', nargs='+',
                        help='name of JSONL files to parse')
    PARSER.add_argument('--core', dest='core', type=str, default=None,
                        help='By default Solr core default name is derived \
                        from the filename')
    PARSER.add_argument('--delete', dest='delete', action='store_true',
                        default=False,
                        help='Delete Solr core')
    ARGS = PARSER.parse_args()
    FILENAMES = ARGS.inputfiles
    CORE = ARGS.core
    DELETE = ARGS.delete
    DEBUG = False

if DEBUG:
    pd.set_option('display.max_columns', None)
    FILENAMES = [FILENAME]
    #BITMAP = True
    #NONUMERIC = True
    #SEQ = 4
    RENAMEID = True
    CORE = 'QX'

for filename in FILENAMES:
    filestub = path.basename(filename)
    this_collection = re.split(r'[\._-]', filestub).pop(0)
    if CORE:
        this_collection = CORE
    if DELETE and ping_collection(this_collection):
        delete_collection(this_collection)        
    if ping_collection(this_collection):
        sys.exit(0)
    if not ping_collection(this_collection):
        create_collection(this_collection)
    #this_schema = get_schema(this_collection)
