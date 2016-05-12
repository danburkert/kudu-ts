# Kudu TS

Kudu TS is a metrics and time series database built on Apache Kudu (incubating).
Kudu's focus on real time data ingestion combined with fast scans makes it an
ideal platform for storing and querying time series data. Kudu TS includes
connectors for popular metrics and alerting tools.

The kernel of Kudu TS lives in the [`core`](core/) module, which provides a Java
API for efficiently storing and querying metrics in Kudu.

The [`KTSD`](ktsd/) module contains a daemon which exposes an HTTP interface to
Kudu TS. KTSD's HTTP interface is a drop-in replacement for OpenTSDB's TSD,
allowing existing tools with OpenTSDB support to use Kudu TS with minimal effort
(such as Grafana).

The [`argus`](argus/) module contains a Kudu TS backend for the
[Argus](https://github.com/SalesforceEng/Argus) metrics and monitoring platform.

## Maturity

Kudu TS is experimental, and does not yet have any stability guarantees.

### Contributions

Contributions (through GitHub pull requests) are encouraged and welcomed. If you
run into any issues, please do not hesitate to open a GitHub issue.

## What is Kudu

[Apache Kudu (incubating)](https://getkudu.io) is a distributed, columnar
storage engine for analytics on fast moving data. Kudu provides real time
ingestion with extremely fast scan capability. Kudu's columnar on-disk format
makes it extremely efficient at scanning, filtering, and aggregating large time
series datasets. The columnar format also provides great compression for time
series datasets. The characterstics make Kudu a great platform for a time series
database.

## License

Kudu TS is distributed under the terms of the Apache License (Version 2.0).

See [LICENSE](LICENSE) for details.
