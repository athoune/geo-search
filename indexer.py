#!/usr/bin/env python
import sys
import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from reader import read
from hierarchy import reverse_ancestor

cpt = 0


settings = {
    'analysis': {
        'analyzer': {
            'myPath': {
                'type': 'custom',
                'tokenizer': 'path_hierarchy'
            }
        }
    }
}


mappings = {
    'geoname': {
        '_all': {
            'enabled': True
        },
        'properties': {
            'name': {
                'type': 'string'
            },
            'hierarchy': {
                'type': 'string',
                'analyzer': 'myPath'
            },
            'location': {
                'type': 'geo_point',
                'validate': True,
                "fielddata": {
                    "format": "compressed",
                    "precision": "3m"
                }
            },
        },
    }
}


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
               'cc': data['country_code'],
               'hierarchy': '/'.join(reverse_ancestor(data['geonameid']))
               }

logger = logging.getLogger('elasticsearch')
logger.addHandler(logging.StreamHandler())
#logger.level = logging.DEBUG

es = Elasticsearch()
if es.indices.exists('geoname'):
    es.indices.delete('geoname')

print es.indices.create(index='geoname', body={
    'mappings': mappings,
    'settings': settings}
)

bulk(es, indexer(read(sys.argv[1])))

es.indices.refresh('geoname')
# 36m31.860s for 8600414 documents
