ElasticSearch Bulk indexer

This small Go library does ElasticSearch Bulk operations only. Actions which resulted in an error will be retried individually. See example_test.go how to use this.

It is tested on the 2.X ElasticSearch series.

For details about Bulk inserts:
http://www.elasticsearch.org/guide/reference/api/bulk.html

# Status

Used in production with millions of documents every day.

# Test

Run `make test` for the basics, no ElasticSearch daemon required.

Run `make livetest` if there is an ElasticSearch available on localhost:9200.
It'll delete the index 'bubbles'.

# Misc

[![Build Status](https://travis-ci.org/realzeitmedia/bubbles.svg?branch=master)](https://travis-ci.org/realzeitmedia/bubbles) 
[![GoDoc](https://godoc.org/github.com/realzeitmedia/bubbles?status.svg)](https://godoc.org/github.com/realzeitmedia/bubbles)
