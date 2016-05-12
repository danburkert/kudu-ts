# Kudu TS Core

The Kudu TS `core` module provides a standard set APIs for storing and querying
metrics data in Kudu.

## Data Model

Kudu TS's data model similar to many other modern metrics stores (e.g. OpenTSDB,
Prometheus). Data is organized into series, each of which has a metric and a set
of tags (arbitrary key value pairs). Each datapoint contains the following
fields:

| field  | type                     |
|--------|--------------------------|
| metric | string                   |
| tagset | map\<string, string\>    |
| time   | timestamp (µs precision) |
| value  | double                   |

## Writing Data

Kudu TS allows data to be inserted out of order, and with `time` values
arbitrarily long in the past. New metrics, tag keys, and tag values can be
inserted without an upfront creation step.

## Querying Data

Kudu TS is optimized for queries that specify a metric and set of tag filters.
Results may be aggregated, downsampled and interpolated using per-query
policies.

## Storage Representation

Kudu TS is designed to provide efficient inserts and scans over the metrics data
points stored in Kudu. The table layouts, partitioning, encoding and compression
options are optimized to provide good performance.

### Table Layout

`core` create 3 tables in Kudu: `metrics`, `tagsets`, and `tags` with the
following schemas:

```sql
CREATE TABLE metrics (metric STRING, tagset_id INT32, time TIMESTAMP, value DOUBLE)
PRIMARY KEY (metric, tagset_id, time)
PARTITION BY HASH (metric, tagset_id) INTO <configurable> BUCKETS
            RANGE (time) WITH SPLIT ROWS <configurable>;

CREATE TABLE tagsets (id INT32, tagset BINARY)
PRIMARY KEY (tagset)
PARTITION BY RANGE(tagset) WITH SPLIT ROWS <configurable by # of partitions>;

CREATE TABLE tags (key STRING, value STRING, tagset_id INT32)
PRIMARY KEY (key, value, tagset_id)
PARTITION BY HASH(key, value) INTO <configurable> BUCKETS;
```

## Roadmap

* Return full tagsets and aggregated tagsets with results
* Group by
* Disjunctive tag filters (`WHERE my_tag = 'foo' OR my_tag = 'bar'`)
* Prefix tag filters (`WHERE my_tag_key LIKE 'foo%'`)
* Annotations
* Metric name search
* Tag key search
* Tag value search
* Upsert datapoints
* Update datapoints
