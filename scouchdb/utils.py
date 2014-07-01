# -*- coding: utf-8 -*-

import json


def encode(value):
    # pylint: disable=E1103
    if not value:
        return
    if isinstance(value, unicode):
        value = value.encode('utf-8')
    if isinstance(value, str):
        v = value
        if v.startswith('[') or v.startswith('{'):
            v = json.loads(v, encoding='utf-8')
        value = v
    if isinstance(value, list):
        _t = []
        for i in value:
            i = encode(i)
            _t.append(i)
        value = _t
    if isinstance(value, dict):
        _d = {}
        for k, v in value.items():
            k = k.encode('utf-8')
            v = encode(v)
            _d[k] = v
        value = _d
    # pylint enable=E1103
    return value
