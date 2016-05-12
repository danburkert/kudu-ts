# KTSD

`ktsd` is a standalone daemon that exposes an HTTP endpoint to Kudu TS. The API
is a drop in replacement for OpenTSDB's TSD HTTP API, which allows KTSD to work
with tools that already support OpenTSDB. KTSD is still a work in progress, not
all of the APIs have been implemented.

## Configuration

KTSD is built on the [Dropwizard framework](dropwizard.github.io), which
provides a standard YAML configuration format. See
[configuration.yml](configuration.yml) for an example configuration including
KTSD specific options, and the Dropwizard configuration reference for general
options.

## Starting KTSD

After building Kudu TS in the top level project directory (see the project
[README](../README.md)), execute:

```bash
java -jar ktsd/target/kudu-ts-tsd-<version>.jar server ktsd/configuration.yml
```

## Load Generator

`KTSD` includes a simple load generator to simulate clients writing metrics data
into Kudu TS through a KTSD instance. The generated metrics, tagsets, and
datapoint times can all be configured. To start a load generator, run:

```bash
java -jar ktsd/target/kudu-ts-tsd-<version>.jar put-bench ktsd/configuration.yml
```
