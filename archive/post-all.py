#!/usr/bin/env python3
"""
post-all.py: parse a JSONL file to create, update and post the
associated core schema and data to a Solr instance
"""
import re
import argparse
import os.path as path
import pandas as pd
from pandas.api.types import infer_dtype, is_datetime64_any_dtype, is_list_like
import numpy as np
from app.flatten_keys import flatten
import warnings
from app.solr import get_schema, set_schema, HTTPError, create_collection,\
    wait_for_success, post_data_api, check_missing_status, get_collections,\
    get_names, error_solr

warnings.simplefilter(action='ignore', category=FutureWarning)

DEBUG = True
FILENAME = 'output/node.jsonl'
FILENAME = 'storage/HD_001.jsonl'
FILENAME = 'nested.jsonl'
FILENAME = 'nested2.jsonl'
FILENAME = 'nested3.jsonl'
FILENAME = 'nested4.jsonl'
FILENAME = 'output/block.jsonl'
FILENAME = 'output/annotation.jsonl'
FILENAME = 'storage/PA_004.jsonl'
FILENAME = 'storage/BS_004.jsonl'
FILENAME = 'block-test.jsonl'
FILENAME = 'storage/PA_004.jsonl'
FILENAME = 'output/coach.jsonl'
FILENAME = 'edge-test.jsonl'
FILENAME = 'output/version.jsonl'
FILENAME = 'storage/AA_003.jsonl'
FILENAME = 'storage/HD_001.jsonl'
FILENAME = 'storage/AA_146.jsonl'
FILENAME = 'storage/TR_002.jsonl'

CORE = None
INTFIELDS = None
NONUMERIC = False
RENAMEID = False
BITMAP = True
SEQ = None
DEFAULTTYPE = 'text_general'
DEFAULTFIELDS = None

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description='Process JSONL file \
file to create, update and post data to Solr')
    PARSER.add_argument('inputfiles', nargs='+',
                        help='name of JSONL files to parse')
    PARSER.add_argument('--core', dest='core', type=str, default=None,
                        help='Solr core default name is derived from \
                        the filename')
    PARSER.add_argument('--week-bitmap', dest='bitmap', action='store_true',
                        default=True,
                        help='Keep week-bitmap string (default: True)')
    PARSER.add_argument('--default-type', dest='default', action='store_true',
                        default='text_general',
                        help='Set default-type (default: text_general)')
    PARSER.add_argument('--no-numeric', dest='nonumeric', action='store_true',
                        default=False,
                        help='No numeric field data (default: False)')
    PARSER.add_argument('--rename-id', dest='rename', action='store_true',
                        default=False,
                        help='If exists rename id field core.id (default: False)')
    PARSER.add_argument('--seq', dest='seq', nargs='?', type=int,
                        help='id sequence number for multiple-file processing')
    PARSER.add_argument('--default-fields', dest='defaultfields', nargs='*',
                        help='list of Solr `default` type field names')
    PARSER.add_argument('--int-fields', dest='intfields', nargs='*',
                        help='list of Solr integer field names')

    ARGS = PARSER.parse_args()
    FILENAMES = ARGS.inputfiles
    CORE = ARGS.core
    BITMAP = ARGS.bitmap
    RENAMEID = ARGS.rename
    DEFAULTTYPE = ARGS.default
    NONUMERIC = ARGS.nonumeric
    DEFAULTFIELDS = ARGS.defaultfields
    INTFIELDS = ARGS.intfields
    SEQ = ARGS.seq
    DEBUG = False

if DEBUG:
    pd.set_option('display.max_columns', None)
    FILENAMES = [FILENAME]
    #BITMAP = True
    NONUMERIC = False
    #SEQ = 4
    RENAMEID = False
    #CORE = 'nested'

def wtt_datetime(this_column):
    """wtt_datetime: convert datatime to ISO8601 UTC/Z string"""
    return pd.to_datetime(this_column).dt.strftime('%Y-%m-%dT%H:%M:%SZ')

def get_solrdtype(this_series):
    try:
        s = pd.to_numeric(this_series)
    except (ValueError, TypeError):
        return DEFAULTTYPE
    if pd.isna(s).any():
        return DEFAULTTYPE
    r = infer_dtype(s)
    if BITMAP and r == 'integer' and (this_series.str.len() == 7).all():
        return DEFAULTTYPE
    if r == 'mixed':
        r = infer_dtype(pd.to_numeric(s) for i in s for j in i)
    return r

def get_type(this_series):
    s = this_series.dropna()
    if s.apply(is_list_like).any():
        s = list(pd.core.common.flatten(s))
    try:
        _ = [i.to_pydatetime() for i in s]
        return 'pdate'
    except AttributeError:
        pass
    p_type = get_solrdtype(s)
    if p_type == 'datetime64':
        return 'pdate'
    try:
        if is_datetime64_any_dtype(s):
            return 'pdate'
    except TypeError:
        pass
    if p_type in ['string', 'text_general']:
        if isinstance(s, list):
            return DEFAULTTYPE
        if np.max(s.fillna('').apply(len)) < 10:
            return DEFAULTTYPE
        if s.str.contains('/').any():
            return DEFAULTTYPE
        try:
            _ = pd.to_datetime(s)
            return 'pdate'
        except (ValueError, TypeError):
            pass
        return DEFAULTTYPE
    if NONUMERIC:
        return DEFAULTTYPE
    if p_type == 'floating':
        return 'pdouble'
    if p_type == 'integer':
        return 'pint'
    raise ValueError('post-all.py: Unable to determine Solr type')

def get_new_schema(this_core, this_df):
    fields = {}
    multi_columns = [c for c in this_df.columns
                     if np.any([isinstance(i, list)
                                for i in this_df[c]])]
    for key in this_df.columns:
        if INTFIELDS and key in INTFIELDS:
            fields[key] = 'pint'
            continue
        if DEFAULTFIELDS and key in DEFAULTFIELDS:
            fields[key] = DEFAULTTYPE
            continue
        fields[key] = get_type(this_df.loc[:, key])
    r = [{'name': key, 'type': fields[key], 'multiValued': True}
         if key in multi_columns else {'name': key, 'type': fields[key]}
         for key in fields if key != 'id']
    return r

def get_df(filename, chunksize=None, dtype=None):
    if chunksize:
        return next(pd.read_json(filename, lines=True, dtype=dtype, chunksize=chunksize))
    return pd.read_json(filename, lines=True, dtype=dtype)

def clean_json(this_df):
    return [{k: v for k, v in m.items() if isinstance(v, list) or pd.notnull(v)} for m in this_df]

def post_chunk(this_df):
    data = clean_json(this_df.to_dict(orient='records'))
    this_response = post_data_api(data, this_core)
    this_header = this_response.pop('responseHeader')
    print({**{'filename': filename}, **this_header})
    if this_header.get('status') != 0:
        error_solr(this_response, this_schema)
    return this_header.get('status') == 0

def post_data(this_df, m=1048576):
    (n, _) = this_df.shape
    i = 0
    for j in range(m, n, m):
        wait_for_success(post_chunk, ConnectionError, this_df.iloc[i:j])
        i = j
    this_header = wait_for_success(post_chunk, ConnectionError, this_df.iloc[i:])
    return this_header

def get_nested_columns(this_df, n=4096):
    df1 = this_df.iloc[:n]
    return [c for c in df1.columns
            if np.any([is_list_like(i) for i in df1[c]])]

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
    fields = schema_v('type', get_schema(name, SOLRMODE))
    update = schema_v('type', this_schema)
    return [i for i in this_schema if i['name'] not in fields or i['type'] != update[i['name']]]

def wait_for_schema(*v):
    return not get_update(*v)

def to_numeric(this_series):
    this_name = this_series.name
    if schema_v('multiValued', this_schema).get(this_name):
        12/0
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

for filename in FILENAMES:
    filestub = path.basename(filename)
    this_core = re.split(r'[\._-]', filestub).pop(0)
    if CORE:
        this_core = CORE
    this_error = None
    this_schema = []
    SOLRMODE = 'collections'
    try:
        _ = get_collections()
    except (ConnectionError, HTTPError) as error:
        this_error = error
    if isinstance(this_error, ConnectionError):
        raise ConnectionError('Error: Solr is not running')
    if isinstance(this_error, HTTPError):
        SOLRMODE = 'cores'
    if this_core not in get_names():
        if SOLRMODE == 'cores':
            raise ConnectionError('Error: create "{}" failed as Solr not\
            running in cloud mode'.format(this_core))
        try:
            create_collection(this_core)
        except HTTPError:
            print('211: {}'.format(filename))
    if not wait_for_success(check_missing_status, (ConnectionError, HTTPError),\
                            this_core, SOLRMODE):
        raise TimeoutError('ERROR: unable to set \
        "add-unknown-fields-to-the-schema" flag {}'.format(this_core))
    df1 = get_df(filename, dtype=object)
    df1 = flatten_df(df1)
    keys = df1.columns.tolist()
    if RENAMEID and 'id' in keys:
        this_id = this_core + '.id'
        if this_id in keys:
            raise ValueError('ERROR: cannot rename field as {} already exists'.format(this_id))
        df1 = df1.rename({'id': this_id}, axis=1)
    if RENAMEID or 'id' not in keys:
        df1['id'] = df1.index
        this_seq = ''
        if SEQ:
            this_seq = str(SEQ).zfill(4) + '.'
        df1['id'] = this_seq + df1['id'].apply(lambda v: str(1 + v).zfill(8))
    this_schema = get_schema(this_core, SOLRMODE)
    new_schema = get_new_schema(this_core, df1.iloc[:4096])
    update = get_update(this_core, new_schema)
    print({SOLRMODE: this_core, 'file': filestub, 'fields': update})
    if update:
        try:
            set_schema(this_core, SOLRMODE, update)
        except HTTPError as error:
            print('237: {}'.format(filename))
            pass
    wait_for_success(wait_for_schema, ConnectionError, this_core, new_schema)
    if update:
        this_schema = get_schema(this_core, SOLRMODE)
    for c, t in schema_v('type', this_schema).items():
        if c not in df1.columns:
            continue
        if t == 'pdate':
            df1[c] = wtt_datetime(df1[c])
        if t in ['pint', 'pdouble']:
            df1[c] = to_numeric(df1[c])
    m = 32768 if SOLRMODE == 'collections' else 1048576
    if not post_data(df1, m):
        raise TimeoutError('unable to post: {}'.format(filename))
