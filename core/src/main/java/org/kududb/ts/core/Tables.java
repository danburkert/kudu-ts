package org.kududb.ts.core;

import com.google.common.collect.ImmutableList;

import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.client.CreateTableOptions;
import org.kududb.client.PartialRow;

/**
 * {@code Tables} holds meta information about the table schemas used by {@code KuduTS}.
 */
@InterfaceAudience.Private
class Tables {

  private Tables() {}

  static final int METRICS_METRIC_INDEX = 0;
  static final int METRICS_TAGSET_ID_INDEX = 1;
  static final int METRICS_TIME_INDEX = 2;
  static final int METRICS_VALUE_INDEX = 3;

  static final int TAGSETS_ID_INDEX = 0;
  static final int TAGSETS_TAGSET_INDEX = 1;

  static final int TAGS_KEY_INDEX = 0;
  static final int TAGS_VALUE_INDEX = 1;
  static final int TAGS_TAGSET_ID_INDEX = 2;

  static final ColumnSchema METRICS_METRIC_COLUMN =
      new ColumnSchema.ColumnSchemaBuilder("metric", Type.STRING)
                      .nullable(false)
                      .key(true)
                      .encoding(ColumnSchema.Encoding.DICT_ENCODING).build();
  static final ColumnSchema METRICS_TAGSET_ID_COLUMN =
      new ColumnSchema.ColumnSchemaBuilder("tagset_id", Type.INT32)
          .nullable(false)
          .key(true)
          .compressionAlgorithm(ColumnSchema.CompressionAlgorithm.LZ4)
                                                                   .build();
  static final ColumnSchema METRICS_TIME_COLUMN =
      new ColumnSchema.ColumnSchemaBuilder("time", Type.TIMESTAMP)
                      .nullable(false)
                      .key(true)
                      .encoding(ColumnSchema.Encoding.BIT_SHUFFLE)
                      .build();
  static final ColumnSchema METRICS_VALUE_COLUMN =
      new ColumnSchema.ColumnSchemaBuilder("value", Type.DOUBLE)
                      .nullable(false)
                      .encoding(ColumnSchema.Encoding.BIT_SHUFFLE)
                      .build();

  static final ColumnSchema TAGSETS_ID_COLUMN =
      new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32)
                      .nullable(false)
                      .key(true).build();
  static final ColumnSchema TAGSETS_TAGSET_COLUMN =
      new ColumnSchema.ColumnSchemaBuilder("tagset", Type.BINARY)
                      .nullable(false)
                      .compressionAlgorithm(ColumnSchema.CompressionAlgorithm.LZ4)
                      .build();

  static final ColumnSchema TAGS_KEY_COLUMN =
      new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING)
                      .nullable(false)
                      .key(true)
                      .encoding(ColumnSchema.Encoding.DICT_ENCODING)
                      .build();
  static final ColumnSchema TAGS_VALUE_COLUMN =
      new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING)
                      .nullable(false)
                      .key(true)
                      .encoding(ColumnSchema.Encoding.DICT_ENCODING)
                      .build();
  static final ColumnSchema TAGS_TAGSET_ID_COLUMN =
      new ColumnSchema.ColumnSchemaBuilder("tagset_id", Type.INT32)
                      .nullable(false)
                      .key(true)
                      .build();

  static final Schema METRICS_SCHEMA = new Schema(ImmutableList.of(METRICS_METRIC_COLUMN,
                                                                   METRICS_TAGSET_ID_COLUMN,
                                                                   METRICS_TIME_COLUMN,
                                                                   METRICS_VALUE_COLUMN));
  static final Schema TAGSETS_SCHEMA = new Schema(ImmutableList.of(TAGSETS_ID_COLUMN,
                                                                   TAGSETS_TAGSET_COLUMN));
  static final Schema TAGS_SCHEMA = new Schema(ImmutableList.of(TAGS_KEY_COLUMN,
                                                                TAGS_VALUE_COLUMN,
                                                                TAGS_TAGSET_ID_COLUMN));

  static String metricsTableName(String tsName) {
    return String.format("kuduts.%s.metrics", tsName);
  }

  static String tagsetsTableName(String tsName) {
    return String.format("kuduts.%s.tagsets", tsName);
  }

  static String tagsTableName(String tsName) {
    return String.format("kuduts.%s.tags", tsName);
  }

  /**
   * The {@code metrics} table is hash partitioned on the {@code metric} and
   * {@code tagset_id} columns, and range partitioned on the {@code time}
   * column. The hash partitioning allows writes and scans at the current time
   * to be evenly distributed over the cluster. Range partitioning on time
   * allows whole tablets to be pruned based on the time constraint, and allows
   * old data to be dropped if desired.
   * @param options the create options
   * @param numTabletServers the number of tablet servers
   * @return the tags table create options
   */
  static CreateTableOptions metricsCreateTableOptions(CreateOptions options,
                                                      int numTabletServers) {
    CreateTableOptions create = new CreateTableOptions();
    create.setNumReplicas(options.getNumReplicas());
    create.addHashPartitions(ImmutableList.of("metric", "tagset_id"),
                             options.getNumMetricsHashBuckets(numTabletServers));
    create.setRangePartitionColumns(ImmutableList.of("time"));
    for (Long time : options.getMetricsSplits()) {
      PartialRow split = METRICS_SCHEMA.newPartialRow();
      split.addLong(METRICS_TIME_INDEX, time);
      create.addSplitRow(split);
    }
    return create;
  }

  /**
   * The {@code tagsets} table is range partitioned on the {@code id} column.
   * Because the table is essentially a linear probe hash table, it must be able
   * to be scanned in PK order, so hash partitioning is not possible. Since the
   * tagset IDs are effectively random, setting split points at even intervals
   * over the ID range gives good protection against hotspotting.
   * @param options the create options
   * @param numTabletServers the number of tablet servers
   * @return the tagset table create options
   */
  static CreateTableOptions tagsetsCreateTableOptions(CreateOptions options,
                                                      int numTabletServers) {
    CreateTableOptions create = new CreateTableOptions();
    create.setNumReplicas(options.getNumReplicas());

    create.setRangePartitionColumns(ImmutableList.of("id"));

    int numTablets = options.getNumTagsetsTablets(numTabletServers);
    long interval = (1L << 32) / numTablets;
    for (int i = 1; i < numTablets; i++) {
      PartialRow split = TAGSETS_SCHEMA.newPartialRow();
      split.addInt(TAGSETS_ID_INDEX, (int) (Integer.MIN_VALUE + i * interval));
      create.addSplitRow(split);
    }
    return create;
  }

  /**
   * The {@code tags} table is hash partitioned on the {@code key} and
   * {@code value} columns, which allows a scan of a ({@code key}, {@code value})
   * pair to be fully satisfied by a single tablet, and gives good protection
   * against hotspotting.
   * @param options the create options
   * @param numTabletServers the number of tablet servers
   * @return the tags table create options
   */
  static CreateTableOptions tagsCreateTableOptions(CreateOptions options,
                                                   int numTabletServers) {
    CreateTableOptions create = new CreateTableOptions();
    create.setNumReplicas(options.getNumReplicas());
    create.addHashPartitions(ImmutableList.of("key", "value"),
                             options.getNumTagsetsTablets(numTabletServers));
    create.setRangePartitionColumns(ImmutableList.<String>of());
    return create;
  }
}
