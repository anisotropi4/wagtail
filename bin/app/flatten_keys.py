"""flatten_keys: A package to flatten keys for use with NoSQL JSON indexing

This is based on the python flatten-dict package modified
(https://github.com/ianlini/flatten-dict)
"""

from collections.abc import Mapping

def flatten(this_dict, sep='.'):
    """Flatten `Mapping` object to a flatten . (dot) separated nested-keys
    to produce a depth-one key dict

    Parameters
    ----------
    this_dict : dict-like object to be flattened.
    Returns
    -------
    out_dict : flattened dict
    """

    flatten_types = (Mapping, list)
    if not isinstance(this_dict, flatten_types):
        raise ValueError('argument type {} is not in the flatten types \
list {}'.format(type(this_dict), flatten_types))

    out_dict = {}

    def _reducer(key_1, key_2):
        if isinstance(key_2, tuple):
            return key_1
        if not key_1:
            return key_2
        return '{}{}{}'.format(key_1, sep, key_2)

    def _flatten(this_dict, parent=None):
        key_value_iterable = (((i,), j) for i, j in enumerate(this_dict)) \
                             if isinstance(this_dict, list) \
                                else this_dict.items()
        for key, value in key_value_iterable:
            this_key = _reducer(parent, key)
            if isinstance(value, flatten_types):
                if value:
                    # recursively build the result
                    _flatten(value, this_key)
                    continue
            # Add value to the result
            if this_key not in out_dict:
                out_dict[this_key] = value
                continue
            if not isinstance(out_dict[this_key], list):
                out_dict[this_key] = [out_dict[this_key]]
            out_dict[this_key] += [value]
    _flatten(this_dict)
    return out_dict
