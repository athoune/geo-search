#!/usr/bin/env python
import sys

from elasticsearch import Elasticsearch


es = Elasticsearch(["localhost"])
print es.suggest({
    'machin-suggest': {
        'text': sys.argv[1],
        'completion': {
            'field': 'suggest'
        }
    }

}, index='geoname')
