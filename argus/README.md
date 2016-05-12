# Argus

This module contains an Argus `TSDBService` implementation backed by Kudu TS.
Currently, the functionality is limited to inserting and querying datapoints,
annotations are not yet supported. To use the service, add a dependency on Kudu
TS Argus in the Argus `pom.xml`, rebuild the project, and configure Argus
to use the `org.kududb.ts.argus.KuduService` TSDB service.

```xml
<dependency>
  <groupId>org.kududb.ts</groupId>
  <artifactId>kudu-ts-argus</artifactId>
  <version><!-- version --></version>
</dependency>
```
