#!/usr/bin/env python

import sys
import codecs
import re

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


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

cpt = 0

def indexer(datas):
    global cpt
    for data in datas:
        cpt += 1
        if cpt % 10000 == 0:
            print ".",
        yield {'name': data['name'],
               '_index': 'geoname',
               '_type': 'stuff',
               '_id': data['geonameid'],
               'cc': data['country_code']}

es = Elasticsearch()
if es.indices.exists('geoname'):
    es.indices.delete('geoname')

bulk(es, indexer(read(sys.argv[1])))

es.indices.refresh('geoname')
# 36m31.860s for 8600414 documents
