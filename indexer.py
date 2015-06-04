#!/usr/bin/env python
import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from reader import read
from hierarchy import reverse_ancestor


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
            'suggest': {
                'type': 'completion',
                'index_analyzer': 'simple',
                'search_analyzer': 'simple',
                'payloads': True
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

cpt = 0


def indexer(datas):
    global cpt
    for data in datas:
        cpt += 1
        if cpt % 10000 == 0:
            print ".",
        yield {
            'name': data['name'],
            'suggest': {
                'input': [data['name']],
            },
            'alternate_name': data['alternatenames'].split(','),
            '_index': 'geoname',
            '_type': 'geoname',
            '_id': data['geonameid'],
            'cc': data['country_code'],
            'hierarchy': '/'.join(reverse_ancestor(data['geonameid'])),
            'location': [data['longitude'], data['latitude']]
        }

if __name__ == '__main__':
    import sys

    logger = logging.getLogger('elasticsearch')
    logger.addHandler(logging.StreamHandler())
    #logger.level = logging.DEBUG

    if len(sys.argv) == 1:
        src = 'FR.txt'
    else:
        src = sys.argv[1]
    if len(sys.argv) > 2:
        nodes = sys.argv[2:]
    else:
        nodes = '127.0.0.1'

    print("nodes", nodes)
    es = Elasticsearch(nodes)

    if es.indices.exists('geoname'):
        es.indices.delete('geoname')

    print es.indices.create(index='geoname', body={
        'mappings': mappings,
        'settings': settings}
    )

    bulk(es, indexer(read(src)))

    es.indices.refresh('geoname')
    # 36m31.860s for 8600414 documents

    print es.indices.stats('geoname')
    #res = es.search(index="geoname", body={"query": {"match_all": {}}})
    #es.search('geoname', {
        #'query': {
            #'filtered': {
                #'filter': {
                    #'term': {'cc': 'FR'}
                #},
                #'query': {
                    #'query_string': {'query': 'Gre%'}
                #}
            #}}})
