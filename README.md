# Kudu TS

Kudu TS is a fast timeseries API for efficiently storing and querying metrics
data in Apache Kudu (incubating) database. Kudu TS is an experimental project,
and does not yet have any stability guarantees. Contributions are welcome.

## Data Model

Kudu TS uses a data model common in modern metrics stores. Datapoints consist
of a few fields:

| field  | type                |
|--------|---------------------|
| metric | string              |
| tagset | map<string, string> |
| time   | timestamp           |
| value  | double              |

The `tagset` field contains a map of arbitrary key value pairs. Each datapoint
is indexed on each of its individual tags, which allows efficient queries which
filter by tag. Kudu TS instances can support hundreds of millions of unique
tagsets.

## License

Kudu TS is distributed under the terms of the Apache License (Version 2.0).

See [LICENSE](LICENSE) for details.
