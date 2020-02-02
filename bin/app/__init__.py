#!/usr/bin/env python3
import sys
from os.path import dirname, abspath

sys.path = sys.path + [dirname(abspath(__file__))]

__all__ = ['SOLRERR', 'HTTPError', 'check_missing_status', 'create_collection', 'delete_collection', 'type_error_solr', 'get_count', 'get_names', 'get_schema', 'ping_name', 'raw_query', 'set_schema', 'wait_for_success', 'get_solrmode']
