Elasticsearch reindex tool
===========================================

Elasticsearch reindex tool provides easy way to rebuild indexes, it is also possible to move indexes between clusters.
Multiple threads are used in order to query(read) and index(write) data. In order to do that efficiently, [scan and scroll](http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/scan-scroll.html) is used to retrieve batches of documents from the old index and then the [bulk API](http://www.elasticsearch.org/guide/en/elasticsearch/client/javascript-api/current/api-reference.html) to push them into new one.

## Elasticsearch version compability

Master branch is compatible with version 2.x

If you wish to use it with version 1.x please checkout branch [1.x](https://github.com/allegro/elasticsearch-reindex-tool/tree/1.x)

## Why another reindex tool?

Our idea was to speed up index rebuilding. To decrease time of reindexing, our tool reads data from old index and writes it to the new one in parallel using multiple threads. To make it possible, each thread reads piece of data from the index based on a chosen field and its values.

Currently tool supports double type and string type fields. 
For double field type queries are spread into segments with given list of thresholds, for string type fields with given prefixes list.

In the future we plan provide more segmentation strategies.

There are similar tools:

 * [npm elasticsearch-reindex](https://www.npmjs.com/package/elasticsearch-reindex) - tool to reindex within one thread
 only, no segmentation implemented, possible to reindex only filtered data
 * [karussell elasticsearch-reindex](https://github.com/karussell/elasticsearch-reindex) - tool to reindex within one
 thread only, no segmentation implemented, possible to reindex only filtered data
 * [geronime es-reindex](https://github.com/geronime/es-reindex) - ruby script to copy and reindex within one thread
 only, no strict typing on data

## Requirements

* JDK 1.8
* ElasticSearch 1.3+

## Usage

First create a package:

`./gradlew jar`

Example of reindex:

**REMEMBER: use elasticsearch binary transport port (by default 9300), not the one used for rest
communication (by default 9200)**

Without segmentation:

`./run.sh -s http://host:9300/index/type -t http://host1:9300/index1/type1  -sc cluster_name -tc
cluster_name1`

With segmentation by double field:

`./run.sh -s http://host:9300/index/type -t http://host1:9300/index1/type1  -sc cluster_name -tc
cluster_name1`

`./run.sh -s http://host:9300/index/type -t http://host1:9300/index1/type1  -sc cluster_name -tc
 cluster_name1 -segmentationField rate.newCoolness -segmentationThresholds 0.0,0.5,0.59,0.6,0.7,0.9,1.0`

 Index querying will divide data into segments based on rate.newCoolness field: (0.0-0.5] (0.5-0.59] (0.59-0.6] (0
 .6-0.7]
 (0.7-0.9],(0.9-1.0]

With segmentation by prefix on string field:

`./run.sh -s http://host:9300/index/type -t http://host1:9300/index1/type1  -sc cluster_name -tc
 cluster_name1 -segmentationField userId -segmentationPrefixes 1,2,3,4,5,6,7`

 In this example index querying will divide data into segments based on the first character of the userId field: 1,2,3,4,5,6,7
 
With query option:

`./run.sh -s http://host:9300/index/type -t http://host1:9300/index1/type1  -sc cluster_name -tc
 cluster_name1 -query "{\"range\": {\"timestamp\" : {\"gte\" : \""2015-12-08T07:03:00.216Z"\"}}}" -sort _timestamp -sortOrder DESC`
 
 In this example index querying will be filtered with query and reindex will take place ordered by sort field and sortOrder 

Options:

    -s, source
       Source f.e. http://localhost:9300/source_index/type
    -sc, source-cluster
       Source cluster name
    -t, target
       Target f.e. http://localhost:9300/target_index/type
    -tc, target-cluster
       Target cluster name
    -segmentationField
       Segmentation field
    -segmentationPrefixes
       Segmentation prefixes (comma-separated)
    -segmentationThresholds
       Segmentation thresholds (only double type)
    -query
       Give a query to filter data 
    -sort
       Give field to sort on (if query option in use)
    -sortOrder
       Give sortOrder  (if query option in use)    
       
`segmentationField`, `segmentationThreshold` and `segmentationPrefixes` are optional parameters, allowing to spread
querying for field with double values or prefix for string field

During reindex process progress message is prompted after each scroll query.

Example of progress message with the time how long it lasts, number of items queried and indexed, occupancy of queue, number of concurrent reader threads and number of failed document indexing:

`11:24:59.567 [pool-1-thread-1] INFO  c.y.e.t.r.s.ProcessStatistics - PT11M43.346S items: 3572086 / 3580842 (10 1)
failed=0`

## License

**Elasticsearch reindex tool** is published under [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).
