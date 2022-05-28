#!/usr/bin/env python3

import os
#import sys
import json
import argparse
import pandas as pd
from filelock import FileLock

#import numpy as np
#import json
#import requests
#from datetime import date
#from dateutil.parser import parse

from app.solr import get_query, get_group, get_schema, set_schema, update_data, wait_for_success, get_solrmode, HTTPError

CORE = None
SOLRMODE = get_solrmode()

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description='Update Solr collection \
    to create location co-ordinate fields')
    PARSER.add_argument(dest='core', type=str, nargs='?', default=None,
                        help='Solr collection default name is derived from \
                        the filename')

    ARGS = PARSER.parse_args()
    CORE = ARGS.core

def trim_f(this_float):
    return '{:.6f}'.format(round(float(this_float), 6))

def wait_for_schema(*v):
    return not get_update(*v)

def schema_v(key, this_schema):
    return {i['name']: i[key] if key in i else {i['name']: False} for i in this_schema}

def get_update(name, this_schema):
    tv_lookup = set({'strings', 'points', 'pdates', 'plongs', 'pdoubles'})
    fields = schema_v('type', get_schema(name, SOLRMODE))
    return [i for i in this_schema if i['name'] not in fields
            or (i['type'] in tv_lookup and fields[i['name']] not in tv_lookup)]

def get_cnames(these_columns):
    n = 0
    (r, c) = ({}, {})
    for i in these_columns:
        if i == 'id': continue
        field_stub, j = i.rsplit('.', 1)
        if j.lower() not in ['longitude', 'latitude']:
            2.1/0
        if field_stub not in c:
            c[field_stub] = str(n).zfill(2)
            n += 1
        if n == 1:
            r[i] = '{}'.format(j.lower())
        else:
            r[i] = '{}_{}'.format(j.lower(), c[field_stub])
    return r

def clean_json(this_df):
    return [{k: v for k, v in m.items() if isinstance(v, list) or v != ''} for m in this_df]

def update_chunk(this_df):
    data = clean_json(this_df.to_dict(orient='records'))
    this_response = update_data(data, CORE)
    this_header = this_response.pop('responseHeader')
    print({**{'collection': CORE}, **this_header})
    if this_header.get('status') != 0:
        HTTPError(this_response)
    return this_header.get('status') == 0

def update_dataframe(this_df, m=1048576):
    (n, _) = this_df.shape
    i = 0
    for j in range(m, n, m):
        wait_for_success(update_chunk, ConnectionError, this_df.iloc[i:j])
        i = j
    this_header = wait_for_success(update_chunk, ConnectionError, this_df.iloc[i:])
    return this_header

print({'collection': CORE, 'field': '_location_'})
SCHEMA = get_schema(CORE, SOLRMODE)
FIELDS = 'id, {}'.format(', '.join([i['name']
                                    for i in SCHEMA
                                    for j in ['longitude', 'latitude']
                                if i['name'].lower().rfind(j) > 0]))
DATA = pd.DataFrame(get_query(CORE, '*:*', fl=FIELDS), dtype=object)

COLUMNS = get_cnames(DATA.columns)

DATA = DATA.rename(columns=COLUMNS)
for c in list(COLUMNS.values()):
    idx1 = DATA[c].notna()
    try:
        DATA.loc[idx1, c] = DATA.loc[idx1, c].apply(trim_f)
    except TypeError:
        DATA = DATA.drop(c, axis=1)
        pass


DATA['_location_'] = DATA['latitude'] + ',' + DATA['longitude']
DATA = DATA.drop(['longitude', 'latitude'], axis=1)

for c in [i for i in DATA.columns if 'latitude_' in i]:
    i = c[-2:]
    idx1 = DATA[c].notna()
    DATA[f'_location_{i}_'] = DATA.loc[idx1, f'latitude_{i}'] + ',' + DATA.loc[idx1, f'longitude_{i}']
    DATA = DATA.drop([f'longitude_{i}', f'latitude_{i}'], axis=1)

DATA = DATA.fillna('')
SCHEMA = [{'name': f'{i}', 'type': 'location'} for i in DATA.columns if i != 'id']
LOCK = FileLock('/tmp/{}-lock'.format(CORE))

with LOCK.acquire(timeout=32):
    UPDATE = get_update(CORE, SCHEMA)
    if UPDATE:
        try:
            set_schema(CORE, SOLRMODE, False, UPDATE)
        except HTTPError as error:
            print('183: {}'.format(filename))
            pass
        wait_for_success(wait_for_schema, ConnectionError, CORE, SCHEMA)

m = 32768
if not update_dataframe(DATA, m):
    raise TimeoutError('unable to post: {}'.format(filename))

