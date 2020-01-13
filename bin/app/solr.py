"""solr: set of `solr` helper functions based on the `pysolr` package"""
import json
from json.decoder import JSONDecodeError as SolrError
import os
from time import sleep
import requests
from requests.exceptions import HTTPError

CONNECTIONS = {}
SOLRHOST = os.environ.get('SOLRHOST', 'localhost')

def get_api(name='', api='', api_type='collections', hostname=SOLRHOST):
    """get_api: get method for v2 Solr api"""
    this_url = 'http://{}:8983/api/{}/{}/{}'\
               .format(hostname, api_type, name, api)
    this_url = this_url.rstrip('/')
    this_request = requests.get(this_url)
    this_request.raise_for_status()
    this_data = this_request.json()
    if api in this_data:
        return this_data[api]
    this_data.pop('responseHeader', None)
    return this_data

def post_api(data, name='', api='', api_type='collections', hostname=SOLRHOST):
    """post_api: post method with v2 Solr api"""
    this_url = 'http://{}:8983/api/{}/{}/{}'\
               .format(hostname, api_type, name, api)
    this_url = this_url.rstrip('/')
    this_request = requests.post(this_url, data)
    this_request.raise_for_status()
    this_data = this_request.json()
    if api in this_data:
        return this_data[api]
    this_data.pop('responseHeader', None)
    return this_data

def delete_api(name='', api='', this_type='collections', hostname=SOLRHOST):
    """delete_api: delete method with v2 Solr api"""
    this_url = 'http://{}:8983/api/{}/{}/{}'\
               .format(hostname, this_type, name, api)
    this_url = this_url.rstrip('/')
    this_request = requests.delete(this_url)
    this_request.raise_for_status()
    this_data = this_request.json()
    if api in this_data:
        return this_data[api]
    this_data.pop('responseHeader', None)
    return this_data

def get_solr(name='', api='', response_header=False, hostname=SOLRHOST):
    """get_solr: get method with v1 Solr api"""
    this_url = 'http://{}:8983/solr/{}/{}'.format(hostname, name, api)
    this_url = this_url.rstrip('/')
    this_request = requests.get(this_url)
    this_data = this_request.json()
    if api in this_data:
        return this_data[api]
    if response_header:
        return this_data
    this_data.pop('responseHeader', None)
    return this_data

def post_solr(data, name='', api='', response_header=False, hostname=SOLRHOST):
    """post_solr: post data method with v1 Solr` api"""
    this_url = 'http://{}:8983/solr/{}/{}'.format(hostname, name, api)
    this_url = this_url.rstrip('/')
    this_request = requests.post(this_url, data)
    this_data = this_request.json()
    if api in this_data:
        return this_data[api]
    if response_header:
        return this_data
    this_data.pop('responseHeader', None)
    return this_data

def raw_query(name, search_str='*:*', sort='id asc', nrows=10, **rest):
    """raw_query: return Solr raw query data for a connection"""
    data = {'q': search_str, 'sort': sort, 'rows': nrows}
    if rest:
        data = {**data, **rest}
    this_response = post_solr(data, name, api='select')
    return this_response

def get_count(name, search_str='*:*', **rest):
    """get_count: return Solr document count for name"""
    if not ping_name(name):
        raise ValueError('"{}" is not a Solr collection or core'.format(name))
    this_response = raw_query(name, q=search_str, start=0, nrows=0, **rest)
    this_data = this_response.pop('response')
    return this_data.pop('numFound')

def clean_query(this_object):
    """clean_query: remove `_version_` key"""
    this_object.pop('_version_', None)
    return this_object

def get_query(solr, search_str, sort='id asc', limitrows=False, nrows=10, **rest):
    """get_query: return Solr query data for `solr` connection"""
    this_data = solr.search(q=search_str, sort=sort, start=0,
                            rows=nrows, **rest)
    output = [clean_query(i) for i in this_data]
    if limitrows:
        return output
    for start in range(nrows, this_data.hits, 1024):
        this_data = solr.search(q=search_str, sort=sort, start=start,
                                rows=1024, **rest)
        output += [clean_query(i) for i in this_data]
    return output

def get_cores():
    """get_cores: return a list of the Solr core names"""
    this_data = get_api(api_type='cores')
    this_status = this_data.get('status')
    return set(this_status.keys())

def error_solr(this_response, this_schema):
    """error_solr: parse response string for field type schema error"""
    error_text = this_response['error']['msg']
    if 'Error adding field' in error_text:
        print(error_text)
        print(error_text.split('\''))
        this_schema = {i['name']: i['type'] for i in this_schema}
        (_, this_field, _, this_data, *_) = error_text.split('\'')
        this_type = this_schema.get(this_field, None)
        raise ValueError('Error: cannot post data "{}" to field "{}" type "{}"'\
                         .format(this_data, this_field, this_type))

def post_data_api(data, name):
    """post_data_api: post data using v1 Solr API"""
    return post_solr(json.dumps(data),
                     name,
                     api='update/json/docs?commit=true',
                     response_header=True)

def get_names():
    """get_names: return a set of Solr collection or core names"""
    try:
        return get_collections()
    except requests.exceptions.HTTPError:
        pass
    try:
        return get_cores()
    except ConnectionError as error:
        print(error)
    return set()

def get_collections():
    """get_collections: return set of collection names"""
    this_data = get_api()
    return set(i for i in this_data['collections'])

def usr_dtype(this_str):
    """usr_dtype: test for system `dtypes`"""
    return this_str.rstrip('_').lstrip('_') == this_str

def get_schema(name, solr_mode='collections', all_fields=False):
    """get_schema: return dict for Solr schema for excluding required and unstored fields"""
    try:
        this_error = None
        this_data = get_api(name, 'schema/fields', solr_mode)
    except HTTPError as error:
        this_error = error
    if isinstance(this_error, HTTPError):
        raise ValueError('"{}" is not a Solr collection or core'.format(name))
    if all_fields:
        return this_data['fields']
    return [i for i in this_data['fields'] if usr_dtype(i['name']) and not i.get('required')]


def solr_field(name=None, type='string', multiValued=False, stored=True):
    """solr_field: convert python dict structure to Solr field structure
    """
    if not name:
        raise TypeError('solar() missing 1 required positional \
        argument: "name"')
    lookup_bool = {True: 'true', False: 'false'}
    return {'name': name, 'type': type,
            'multiValued': lookup_bool[multiValued],
            'stored': lookup_bool[stored]}

def set_schema(name, solr_mode='collections', *v):
    """set_schema: add or replace the Solr schema for name from list of dict
    `v` containing `name` and `type` keys"""
    fields = []
    for i in v:
        if isinstance(i, list):
            fields += i
        else:
            fields.append(i)
    schema_fields = None
    try:
        schema_fields = {i['name']: i['type'] for i in get_schema(name, solr_mode)}
    except ValueError:
        pass
    data = {'add-field': [], 'replace-field': []}
    for field in fields:
        if field['name'] in schema_fields:
            data['replace-field'].append(solr_field(**field))
            continue
        data['add-field'].append(solr_field(**field))
    return post_solr(json.dumps(data), name, api='schema')

def wait_for_success(function, error, *rest):
    """wait_for_success: poll for successful completion of function"""
    for i in range(64):
        try:
            if function(*rest):
                return True
        except error:
            pass
        print({'waiting': function.__name__, 'count': i})
        sleep(1.0)
    return False

def check_missing_status(name, solr_mode='collections', status='false'):
    """check_missing_status: test if `add-unknown-fields-to-the-schema`
    parameter is set"""
    this_result = get_api(name, 'config/updateRequestProcessorChain', solr_mode)
    this_data = this_result['config']['updateRequestProcessorChain']
    return  all(i['default'] == status
                for i in this_data
                if i['name'] == 'add-unknown-fields-to-the-schema')

def create_collection(name, shards=1, replication=1):
    """create_collection: create Solr collection"""
#def create_collection(name, hostname=SOLRHOST, shards=3, replication=2):
    print('create collection {}'.format(name))
    data = {'create': {'name': name,
                       'numShards': shards,
                       'replicationFactor':replication,
                       'waitForFinalState': 'true'}}
    this_response = post_api(json.dumps(data))
    print(this_response)
    print('created collection {}'.format(name))
    data = {'set-user-property': {'update.autoCreateFields': 'false',
                                  'waitForFinalState': 'true'}}
    post_api(json.dumps(data), name, 'config')
    print('autoCreateFields {}'.format(name))

def delete_schema(name):
    """remove_schema: remove field definitions for Solr schema `name`"""
    fields = [{'name': i['name']} for i in get_schema(name)]
    data = {'delete-field': fields}
    return post_solr(json.dumps(data), name, api='schema')

def delete_collection(name, drop_schema=True):
    """delete_collection: delete collection and optionally drop schema"""
    print('delete collection {}'.format(name))
    if drop_schema and get_schema(name):
        print('delete schema {}'.format(name))
        delete_schema(name)
        print('deleted schema {}'.format(name))
    delete_api(name)
    wait_for_success(lambda v: not ping_name(v), ValueError, name)
    print('deleted collection {}'.format(name))

def ping_name(name, solr_mode='cores'):
    """ping_name: check if collection or core exists"""
    this_api = 'admin/ping' if solr_mode == 'cores' else 'admin/ping?distrib=true'
    try:
        this_data = get_solr(name, api=this_api)
    except (HTTPError, SolrError, ConnectionError):
        return False
    return this_data.get('status') == 'OK'
