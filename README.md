Geo-search
==========

A POC for indexing geonames datas with elasticsearch.

Test it
-------

Virtualenv dance.

    $ virtualenv .
    $ ./bin/pip install -r requirements.txt

Fetch some datas from http://download.geonames.org/export/dump/ download allCountries.zip or a smaller file, one of the XX.zip files.
Unzip it.

Launch elasticsearch.

Index. Enjoy your hardware benchmark.

    $ ./bin/python reader.py allCountries.txt
