#!/usr/bin/env python3
import sys
from os.path import dirname, abspath

sys.path = sys.path + [dirname(abspath(__file__))]

__all__ = ['check_missing_status', 'create_collection', 'delete_collection', 'delete_schema', 'error_solr', 'get_count', 'get_names', 'get_schema', 'ping_name', 'raw_query', 'set_schema', 'wait_for_success']
