ElasticSearch Bulk indexer

This small library does ElasticSearch Bulk operations only. Actions which resulted in an error will be retried individually. See example_test.go how to use this.

http://www.elasticsearch.org/guide/reference/api/bulk.html

# Status

Work in progress. It aims for the 1.4.X ElasticSearch series (the current one).

# Test

Run `make test` for the basics, no ElasticSearch daemon required.

Run `make livetest` if there is an ElasticSearch available on localhost:9200.
It'll delete the index 'bubbles'.

# TODO

Requeue error 429s:
http://XX.XX.XX.XX:9200/_bulk: update error 429: EsRejectedExecutionException[rejected execution (queue capacity 50) on org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction$AsyncShardOperationAction$1@713e81b2]

