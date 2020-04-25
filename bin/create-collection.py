#!/usr/bin/env python3
"""
create-collection.py: create a Solr collection instance
"""

import json
import argparse
import sys

from app.solr import create_collection, delete_collection, ping_name, wait_for_success

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
    PARSER = argparse.ArgumentParser(description='Create Solr collection')
    PARSER.add_argument('collection', nargs=1,
                        help='name of collection to create')
    PARSER.add_argument('--delete', dest='delete', action='store_true',
                        default=False,
                        help='delete existing Solr collection first')
    PARSER.add_argument('--no-autoschema', dest='noschema', action='store_true',
                        default=False,
                        help='Don\'t set "autoCreateFields" for schema')

    ARGS = PARSER.parse_args()
    CORE = ARGS.collection.pop()
    DELETE = ARGS.delete
    SCHEMA = not ARGS.noschema
    DEBUG = False

if DEBUG:
    pd.set_option('display.max_columns', None)
    #BITMAP = True
    #NONUMERIC = True
    #SEQ = 4
    RENAMEID = True
    CORE = 'QX'

if DELETE and ping_name(CORE, solr_mode='collections'):
    delete_collection(CORE)

if ping_name(CORE, solr_mode='collections'):
    sys.exit(0)

if not ping_name(CORE, solr_mode='collections'):
    create_collection(CORE, set_schema=SCHEMA)
