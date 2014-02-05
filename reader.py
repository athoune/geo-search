#!/usr/bin/env python

import codecs
import re


def int_or_none(a):
    if a == '':
        return None
    return int(a)


def read(path):
    keys = re.split(r"\s+", "geonameid name asciiname alternatenames latitude \
        longitude feature_class feature_code country_code cc2 admin1 admin2 \
        admin3 admin4 population elevation dem timezone modification")
    with codecs.open(path, 'r', 'utf8') as f:
        for line in f:
            values = dict(zip(keys, line[:-1].split('\t')))
            values['latitude'] = float(values['latitude'])
            values['longitude'] = float(values['longitude'])
            values['population'] = int_or_none(values['population'])
            values['elevation'] = int_or_none(values['elevation'])
            yield values
