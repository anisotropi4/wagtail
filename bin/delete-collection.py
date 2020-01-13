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

from app.flatten_keys import flatten
from app.solr import ping_name, delete_collection

DEBUG = True
CORE = None

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description='Delete Solr collection')
    PARSER.add_argument('collection', nargs=1, type=str)
    ARGS = PARSER.parse_args()
    CORE = ARGS.collection.pop()
    DEBUG = False

if DEBUG:
    pd.set_option('display.max_columns', None)
    #BITMAP = True
    #NONUMERIC = True
    #SEQ = 4
    RENAMEID = True
    CORE = 'QX'

if ping_name(CORE):
    delete_collection(CORE)
    
