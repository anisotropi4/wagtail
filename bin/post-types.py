#!/usr/bin/env python3
"""
post-types.py: parse a JSONL file to create, update and post the
associated core/collection schema and data to a Solr instance
"""
import re
import argparse
import os.path as path
from os import getcwd
import pandas as pd
from pandas.api.types import is_list_like
import numpy as np
from app.flatten_keys import flatten
import warnings
import app.solr as solr
from filelock import FileLock

CORE = None
INTFIELDS = None
NONUMERIC = False
RENAMEID = False
BITMAP = True
SEQ = None
#DEFAULTTYPE = 'text_general'
DEFAULTTYPE = 'string'
DEFAULTFIELDS = None
CWD = getcwd()
NOPOST = False
DEBUG = False

try:
    from debug import FILENAMES, RENAMEID, CWD, CORE, NOPOST
    DEBUG = True
except ModuleNotFoundError:
    pass

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
    PARSER.add_argument('--set-schema', dest='nopost', action='store_true',
                        default=False,
                        help='do not post data')

    ARGS = PARSER.parse_args()
    FILENAMES = ARGS.inputfiles
    CORE = ARGS.core
    RENAMEID = ARGS.rename
    SEQ = ARGS.seq
    NOPOST = ARGS.nopost
    if DEBUG:
        raise RuntimeError(('ERROR post-types.py: running command line '
                            'in debug mode'))

def is_any_lists(this_series):
    return np.any([is_list_like(i) for i in this_series])

def get_nested_columns(this_df):
    return set(c for c in this_df.columns if is_any_lists(this_df[c]))

def get_seriestype(this_series):
    try:
        return pd.to_numeric(this_series[this_series != '']).dtype
    except (ValueError, TypeError):
        pass
    return np.dtype('object')

def is_zerospcpadded(this_series):
    s = this_series.explode().drop_duplicates()
    s = s[(s != '') & (s != '0.0') & (s != '0')]
    try:
        n = set(s.str.len())
    except AttributeError:
        return False
    r = s.str[0]
    return ((r == '0') | (r == ' ')).any()

def is_sparse(this_series):
    try:
        if this_series.astype(str).str.len().max() > 3:
            return False
        n = this_series.shape[0]
        m = np.sum(this_series.str.count('\d+') > 0)
        return n > 2 * m
    except:
        return False

def is_anychar(this_series, c):
    return (this_series.str.find(c) > 0).any()

def is_allchar(this_series, c):
    return (this_series.str.find(c) > 0).all()

def is_solrdt(this_series):
    if is_any_lists(this_series):
        return False
    if not this_series.str.endswith('Z').all():
        return False
    if (this_series.str.len() != 20).all():
        return False
    try:
        pd.to_datetime(this_series, format='%Y-%m-%dT%H:%M:%SZ')
        return True
    except ValueError:
        return False

def is_location(this_series):
    if not is_allchar(this_series, ','):
        return False
    try:
        this_df = this_series.str.replace('-0', '0').str.split(r'[.,]', expand=True)
        if this_df.shape[1] != 4:
            return False
        this_df.astype(int)
        return True
    except ValueError:
        return False

def get_fieldtypes(this_df):
    field_types = {c: DEFAULTTYPE for c in this_df.columns}
    for c in this_df.columns:
        if is_zerospcpadded(this_df[c]) or is_sparse(this_df[c]):
            field_types[c] = 'string'
            continue
        this_type = get_seriestype(this_df[c])
        if this_type is np.dtype('float'):
            field_types[c] = 'pdouble'
            continue
        if this_type is np.dtype('int'):
            field_types[c] = 'plong'
            continue
        if is_location(this_df[c]):
            field_types[c] = 'location'
            continue
        if is_solrdt(this_df[c]):
            field_types[c] = 'pdate'
            continue
        if not is_anychar(this_df[c], ' '):
            field_types[c] = 'string'
            continue
    return field_types

def get_type(this_type, multi_column):
    sv_lookup = set({'string', 'point', 'pdate', 'plong', 'pdouble'})
    if this_type in sv_lookup and multi_column:
        return this_type + 's'
    return this_type

def get_new_schema(this_core, this_df):
    multi_columns = get_nested_columns(this_df)
    field_types = get_fieldtypes(this_df)
    fields = [{'name': key,
               'type': get_type(field_types[key], key in multi_columns)}
               for key in this_df.columns if key != 'id']
    return fields

def get_df(filename, chunksize=None, dtype=None):
    if chunksize:
        return next(pd.read_json(filename, lines=True, dtype=dtype, chunksize=chunksize), convert_dates=[])
    return pd.read_json(filename, lines=True, dtype=dtype, convert_dates=[])

def clean_json(this_df):
    return [{k: v for k, v in m.items() if isinstance(v, list) or v != ''} for m in this_df]

def post_chunk(this_df):
    data = clean_json(this_df.to_dict(orient='records'))
    this_response = solr.post_data(data, this_core)
    this_header = this_response.pop('responseHeader')
    print({**{'filename': filename}, **this_header})
    if this_header.get('status') != 0:
        solr.HTTPError(this_response)
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
    return {i['name']: i[key] if key in i else {i['name']: False} for i in this_schema}

def get_update(name, this_schema):
    tv_lookup = set({'strings', 'points', 'pdates', 'plongs', 'pdoubles'})
    nv_lookup = set({'plong', 'pdouble'})
    fields = schema_v('type', solr.get_schema(name, SOLRMODE))
    return [i for i in this_schema if i['name'] not in fields
            or (i['type'] in tv_lookup and fields[i['name']] not in tv_lookup)]

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
    return this_df.rename({'id': this_id}, axis=1)

def set_id(this_df):
    this_df['id'] = this_df.index
    this_seq = ''
    if SEQ:
        this_seq = str(SEQ).zfill(4) + '.'
    return this_seq + this_df['id'].apply(lambda v: str(1 + v).zfill(8))

for filename in FILENAMES:
    filestub = path.basename(filename)
    filename = path.join(CWD, filename)
    this_core = re.split(r'[\._-]', filestub).pop(0)
    if CORE:
        this_core = CORE
    SOLRMODE = solr.get_solrmode()
    lock = FileLock('/tmp/{}-lock'.format(this_core))
    with lock.acquire(timeout=32):
        if this_core not in solr.get_names():
            create_collection(this_core)
            if not solr.wait_for_success(solr.check_missing_status, \
                                         (ConnectionError, solr.HTTPError), \
                                         this_core):
                raise TimeoutError('ERROR: "collection {} create failed"'.format(this_core))
            print('collection {} created'.format(this_core))
    df1 = get_df(filename, dtype=object)
    df1 = flatten_df(df1)
    keys = df1.columns.tolist()
    if RENAMEID and 'id' in keys:
        df1 = rename_id(df1)
    if RENAMEID or 'id' not in keys:
        df1['id'] = set_id(df1)
    df1 = df1.fillna('', downcast=object)
    if NOPOST:
        new_schema = get_new_schema(this_core, df1)
        with lock.acquire(timeout=32):
            update = get_update(this_core, new_schema)
            print({SOLRMODE: this_core, 'file': filestub, 'fields': update})
            if update:
                print({'updating': update})
                try:
                    solr.set_schema(this_core, SOLRMODE, False, update)
                except solr.HTTPError as error:
                    print('183: {}'.format(filename))
                    pass
                solr.wait_for_success(wait_for_schema, ConnectionError, this_core, new_schema)
    if not NOPOST:
        print({SOLRMODE: this_core, 'file': filestub, 'fields': 'post'})
        m = 32768 if SOLRMODE == 'collections' else 1048576
        if not post_data(df1, m):
            raise TimeoutError('unable to post: {}'.format(filename))
