#!/usr/bin/env python3
"""
post-all.py: parse a JSONL file to create, update and post the
associated core schema and data to a Solr instance
"""
import re
import argparse
import os.path as path
from os import getcwd
import pandas as pd
import numpy as np
from app.flatten_keys import flatten
import warnings
import app.solr as solr

CORE = None
INTFIELDS = None
NONUMERIC = False
RENAMEID = False
BITMAP = True
SEQ = None
DEFAULTTYPE = 'text_general'
DEFAULTFIELDS = None

DEBUG = True
if __name__ == '__main__':
    warnings.simplefilter(action='ignore', category=FutureWarning)

    PARSER = argparse.ArgumentParser(description='Process JSONL file \
    file to create, update and post data to Solr')
    PARSER.add_argument('inputfiles', nargs='+',
                        help='name of JSONL files to parse')
    PARSER.add_argument('--core', dest='core', type=str, default=None,
                        help='Solr core default name is derived from \
                        the filename')
    PARSER.add_argument('--seq', dest='seq', nargs='?', type=int,
                        help='id sequence number for multiple-file processing')
    PARSER.add_argument('--rename-id', dest='rename', action='store_true',
                        default=False,
                        help='rename id field to core.id if it exists')

    ARGS = PARSER.parse_args()
    FILENAMES = ARGS.inputfiles
    CORE = ARGS.core
    RENAMEID = ARGS.rename
    SEQ = ARGS.seq
    DEBUG = False

CWD = getcwd()

if DEBUG:
    try:
        from debug import FILENAMES, RENAMEID, CWD
    except ModuleNotFoundError:
        pass

def get_nested_columns(this_df):
    return set(c for c in df1.columns
            if np.any([is_list_like(i) for i in this_df[c]]))

def get_new_schema(this_core, this_df):
    multi_columns = get_nested_columns(this_df)
    fields = [{'name': key, 'type': DEFAULTTYPE, 'multiValued': True}
         if key in multi_columns else {'name': key, 'type': DEFAULTTYPE}
         for key in this_df.columns if key != 'id']
    return fields

def get_df(filename, chunksize=None, dtype=None):
    if chunksize:
        return next(pd.read_json(filename, lines=True, dtype=dtype, chunksize=chunksize), convert_dates=[])
    return pd.read_json(filename, lines=True, dtype=dtype, convert_dates=[])

def clean_json(this_df):
    return [{k: v for k, v in m.items() if isinstance(v, list) or pd.notnull(v)} for m in this_df]

def post_chunk(this_df):
    data = clean_json(this_df.to_dict(orient='records'))
    this_response = solr.post_data(data, this_core)
    this_header = this_response.pop('responseHeader')
    print({**{'filename': filename}, **this_header})
    if this_header.get('status') != 0:
        solr.HTTPError(this_response, this_schema)
    return this_header.get('status') == 0

def post_data(this_df, m=1048576):
    (n, _) = this_df.shape
    i = 0
    for j in range(m, n, m):
        solr.wait_for_success(post_chunk, ConnectionError, this_df.iloc[i:j])
        i = j
    this_header = solr.wait_for_success(post_chunk, ConnectionError, this_df.iloc[i:])
    return this_header

def flatten_df(this_df):
    nested_columns = get_nested_columns(this_df)
    for c in nested_columns:
        this_data = this_df[c].apply(lambda x: flatten({c: x})).to_list()
        this_df = this_df.drop(c, axis=1)
        df2 = pd.DataFrame.from_dict(this_data)
        df2 = df2.dropna(axis=1, how='all')
        this_df = this_df.join(df2)
    return this_df

def schema_v(key, this_schema):
    return {i['name']: i[key] for i in this_schema}

def get_update(name, this_schema):
    fields = schema_v('type', solr.get_schema(name, SOLRMODE))
    update = schema_v('type', this_schema)
    return [i for i in this_schema if i['name'] not in fields or i['type'] != update[i['name']]]

def wait_for_schema(*v):
    return not get_update(*v)

def to_numeric(this_series):
    this_name = this_series.name
    if schema_v('multiValued', this_schema).get(this_name):
        return this_series
    this_error = None
    try:
        this_df = pd.to_numeric(this_series)
    except ValueError as error:
        this_error = error
    if this_error:
        print(this_error)
        (_, this_data, *_) = this_error.args[0].split('\"')
        raise TypeError('ERROR: cannot post data "{}" to field "{}" type "{}"'\
                        .format(this_data, c, t))
    return this_df

def create_collection(this_core):
    if SOLRMODE == 'cores':
        raise ConnectionError('Error: create "{}" failed as Solr not\
        running in cloud mode'.format(this_core))
    try:
        solr.create_collection(this_core)
    except solr.HTTPError:
        print('139: {}'.format(filename))

def rename_id(this_df):
    this_id = this_core + '.id'
    if this_id in keys:
        raise ValueError('ERROR: cannot rename field as {} already exists'.format(this_id))
    return df1.rename({'id': this_id}, axis=1)

def set_id(this_df):
    df1['id'] = df1.index
    this_seq = ''
    if SEQ:
        this_seq = str(SEQ).zfill(4) + '.'
    return this_seq + df1['id'].apply(lambda v: str(1 + v).zfill(8))

for filename in FILENAMES:
    filestub = path.basename(filename)
    filename = path.join(CWD, filename)
    this_core = re.split(r'[\._-]', filestub).pop(0)
    if CORE:
        this_core = CORE
    SOLRMODE = solr.get_solrmode()
    if this_core not in solr.get_names():
        create_collection(this_core)
    if not solr.wait_for_success(solr.check_missing_status, \
                                 (ConnectionError, solr.HTTPError), \
                                 this_core, SOLRMODE):
        raise TimeoutError('ERROR: "add-unknown-fields-to-the-schema" \
        flag is not set for {}'.format(this_core))
    df1 = get_df(filename, dtype=object)
    df1 = flatten_df(df1)
    keys = df1.columns.tolist()
    if RENAMEID and 'id' in keys:
        df1 = rename_id(df1)
    if RENAMEID or 'id' not in keys:
        df1['id'] = set_id(df1)
    this_schema = solr.get_schema(this_core, SOLRMODE)
    new_schema = get_new_schema(this_core, df1)
    update = get_update(this_core, new_schema)
    print({SOLRMODE: this_core, 'file': filestub, 'fields': update})
    if update:
        try:
            solr.set_schema(this_core, SOLRMODE, update)
        except solr.HTTPError as error:
            print('183: {}'.format(filename))
            pass
    solr.wait_for_success(wait_for_schema, ConnectionError, this_core, new_schema)
    if update:
        this_schema = solr.get_schema(this_core, SOLRMODE)
    m = 32768 if SOLRMODE == 'collections' else 1048576
    if not post_data(df1, m):
        raise TimeoutError('unable to post: {}'.format(filename))
