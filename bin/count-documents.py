#!/usr/bin/env python3
"""
create-collection.py: create a Solr collection instance
"""
import json
import argparse

from app.solr import get_count, HTTPError

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
    PARSER = argparse.ArgumentParser(description='Number of documents in Solr collection')
    PARSER.add_argument('collection', nargs=1, type=str)
    ARGS = PARSER.parse_args()
    CORE = ARGS.collection.pop()
    DEBUG = False

if DEBUG:
    FILENAMES = [FILENAME]
    #BITMAP = True
    #NONUMERIC = True
    #SEQ = 4
    RENAMEID = True
    CORE = 'TR'

THIS_COUNT = 'missing'
try:
    THIS_COUNT = get_count(CORE)
except (ValueError, HTTPError):
    pass
THIS_DATA = {CORE: THIS_COUNT}
print(json.dumps(THIS_DATA))
