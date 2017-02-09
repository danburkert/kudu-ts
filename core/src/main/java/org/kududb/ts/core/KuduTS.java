package org.kududb.ts.core;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.kudu.Schema;
import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.annotations.InterfaceStability;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Public
@InterfaceStability.Unstable
@ThreadSafe
public class KuduTS implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(KuduTS.class);

  private final AsyncKuduClient client;
  private String name;

  private final Metrics metrics;
  private final Tagsets tagsets;
  private final Tags tags;

  private KuduTS(AsyncKuduClient client,
                 String name,
                 Metrics metrics,
                 Tagsets tagsets,
                 Tags tags) {
    this.client = client;
    this.name = name;
    this.metrics = metrics;
    this.tags = tags;
    this.tagsets = tagsets;
  }

  /**
   * Opens a Kudu TS instance on a Kudu cluster.
   *
   * @param kuduMasterAddressess list of "host:port" pair master addresses
   * @param name the name of the Kudu timeseries store. Multiple instances of
   *             Kudu TS can occupy the same Kudu cluster by using a different name.
   * @return the opened {@code KuduTS}.
   * @throws Exception on error
   */
  public static KuduTS open(List<String> kuduMasterAddressess, String name) throws Exception {
    AsyncKuduClient client = new AsyncKuduClient.AsyncKuduClientBuilder(kuduMasterAddressess).build();

    Deferred<KuduTable> metricsDeferred = client.openTable(Tables.metricsTableName(name));
    Deferred<KuduTable> tagsetsDeferred = client.openTable(Tables.tagsetsTableName(name));
    Deferred<KuduTable> tagsDeferred = client.openTable(Tables.tagsTableName(name));
    KuduTable metricsTable = metricsDeferred.join(client.getDefaultAdminOperationTimeoutMs());
    KuduTable tagsetsTable = tagsetsDeferred.join(client.getDefaultAdminOperationTimeoutMs());
    KuduTable tagsTable = tagsDeferred.join(client.getDefaultAdminOperationTimeoutMs());

    Tags tags = new Tags(client, tagsTable);
    Tagsets tagsets = new Tagsets(client, tags, tagsetsTable);
    Metrics metrics = new Metrics(client, metricsTable, tagsets);
    return new KuduTS(client, name, metrics, tagsets, tags);
  }

  /**
   * Creates (if necessary) and opens a Kudu TS instance on a Kudu cluster.
   *
   * @param kuduMasterAddressess list of "host:port" pair master addresses
   * @param name the name of the Kudu timeseries store. Multiple instances of
   *             Kudu TS can occupy the same Kudu cluster by using a different name.
   * @return the opened {@code KuduTS}.
   * @throws Exception on error
   */
  public static KuduTS openOrCreate(List<String> kuduMasterAddressess,
                                    String name,
                                    CreateOptions options) throws Exception {

    AsyncKuduClient client = new AsyncKuduClient.AsyncKuduClientBuilder(kuduMasterAddressess).build();

    int numTabletServers = client.listTabletServers()
                                 .joinUninterruptibly(client.getDefaultAdminOperationTimeoutMs())
                                 .getTabletServersCount();

    Deferred<KuduTable> metricsDeferred = openOrCreateTable(client,
                                                            Tables.metricsTableName(name),
                                                            Tables.METRICS_SCHEMA,
                                                            Tables.metricsCreateTableOptions(options, numTabletServers));
    Deferred<KuduTable> tagsetsDeferred = openOrCreateTable(client,
                                                            Tables.tagsetsTableName(name),
                                                            Tables.TAGSETS_SCHEMA,
                                                            Tables.tagsetsCreateTableOptions(options, numTabletServers));
    Deferred<KuduTable> tagsDeferred = openOrCreateTable(client,
                                                         Tables.tagsTableName(name),
                                                         Tables.TAGS_SCHEMA,
                                                         Tables.tagsCreateTableOptions(options, numTabletServers));
    KuduTable metricsTable = metricsDeferred.join(client.getDefaultAdminOperationTimeoutMs());
    KuduTable tagsetsTable = tagsetsDeferred.join(client.getDefaultAdminOperationTimeoutMs());
    KuduTable tagsTable = tagsDeferred.join(client.getDefaultAdminOperationTimeoutMs());

    Tags tags = new Tags(client, tagsTable);
    Tagsets tagsets = new Tagsets(client, tags, tagsetsTable);
    Metrics metrics = new Metrics(client, metricsTable, tagsets);
    return new KuduTS(client, name, metrics, tagsets, tags);
  }

  private static Deferred<KuduTable> openOrCreateTable(final AsyncKuduClient client,
                                                       final String table,
                                                       final Schema schema,
                                                       final CreateTableOptions options) throws Exception {
    class CreateTableErrback implements Callback<Deferred<KuduTable>, Exception> {
      @Override
      public Deferred<KuduTable> call(Exception e) throws Exception {
        // TODO(danburkert): we should only do this if the error is "not found"
        LOG.debug("Creating table {}", table);
        return client.createTable(table, schema, options);
      }
      @Override
      public String toString() {
        return MoreObjects.toStringHelper(this).add("table", table).toString();
      }
    }

    return client.openTable(table).addErrback(new CreateTableErrback());
  }

  /**
   * Create a new {@link WriteBatch} for inserting datapoints into the timeseries table.
   * @return a new {@code WriteBatch}
   */
  public WriteBatch writeBatch() {
    return new WriteBatch(this);
  }

  /**
   * Query the timeseries table.
   * If there are no results for the query, the returned list will be empty.
   *
   * @param query parameters
   * @return the queried datapoints
   * @throws Exception on error
   */
  public List<Datapoints> query(final Query query) throws Exception {
    class ScanSeriesCB implements Callback<Deferred<ArrayList<Datapoints>>, IntVec> {
      @Override
      public Deferred<ArrayList<Datapoints>> call(IntVec tagsetIDs) {
        List<Deferred<Datapoints>> series = new ArrayList<>(tagsetIDs.len());
        IntVec.Iterator iter = tagsetIDs.iterator();
        while (iter.hasNext()) {
          series.add(metrics.scanSeries(query.getMetric(),
                                        iter.next(),
                                        query.getStart(),
                                        query.getEnd(),
                                        query.getDownsampler(),
                                        query.getDownsampleInterval()));
        }
        return Deferred.group(series);
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("query", query)
                          .toString();
      }
    }

    List<Datapoints> series = tags.getTagsetIDsForTags(query.getTags())
                                  .addCallbackDeferring(new ScanSeriesCB())
                                  .joinUninterruptibly(client.getDefaultOperationTimeoutMs());

    // Filter empty series
    Iterator<Datapoints> seriesIter = series.iterator();
    while (seriesIter.hasNext()) {
      if (seriesIter.next().size() == 0) seriesIter.remove();
    }

    if (series.isEmpty()) { return ImmutableList.of(); }

    if (query.getInterpolator() == null) {
      return ImmutableList.of(Datapoints.aggregate(series, query.getAggregator()));
    } else {
      return ImmutableList.of(Datapoints.aggregate(series, query.getAggregator(),
                                                   query.getInterpolator()));
    }
  }

  /**
   * Returns the {@link AsyncKuduClient} being used by this {@code KuduTS}.
   * Package private because the client is internal.
   * @return the client instance
   */
  AsyncKuduClient getClient() {
    return client;
  }

  /**
   * Returns the {@link Metrics} being used by this {@code KuduTS}.
   * Package private because the {@code Metrics} is internal.
   * @return the metrics instance
   */
  Metrics getMetrics() {
    return metrics;
  }

  /**
   * Returns the {@link Tags} being used by this {@code KuduTS}.
   * Package private because the {@code Tags} is internal.
   * @return the tags instance
   */
  Tags getTags() {
    return tags;
  }

  /**
   * Returns the {@link Tagsets} being used by this {@code KuduTS}.
   * Package private because the {@code Tagsets} is internal.
   * @return the tagsets instance
   */
  Tagsets getTagsets() {
    return tagsets;
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws Exception {
    client.close();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", name).toString();
  }
}
