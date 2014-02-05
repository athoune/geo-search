import sys

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from reader import read
from hierarchy import reverse_ancestor

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
               'cc': data['country_code'],
               'hierarchy': '/'.join(reverse_ancestor(data['geonameid']))
               }


es = Elasticsearch()
if es.indices.exists('geoname'):
    es.indices.delete('geoname')

bulk(es, indexer(read(sys.argv[1])))

es.indices.refresh('geoname')
# 36m31.860s for 8600414 documents
