package org.kududb.ts.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Unstable
@NotThreadSafe
public class CreateOptions {

  private int numTagsTablets = 0;
  private double numTagsTabletsPerTabletServer = 1.0;

  private int numTagsetsTablets = 0;
  private double numTagsetsTabletsPerTabletServer = 1.0;

  private int numMetricsHashBuckets = 0;
  private double numMetricsHashBucketsPerTabletServer = 3.0;

  private List<Long> metricsSplits = ImmutableList.of();

  private int numReplicas = 3;

  private CreateOptions() {}

  /**
   * Returns the default create options:
   *
   * <ul>
   *   <li>numReplicas: 3</li>
   *   <li>tags: 1 tablet per tablet server</li>
   *   <li>tagsets: 1 tablet per tablet server</li>
   *   <li>metrics: 3 tablets per tablet server, no time partitioning</li>
   * </ul>
   *
   * @return the create options
   */
  public static CreateOptions defaults() {
    return new CreateOptions();
  }

  public CreateOptions setNumReplicas(int numReplicas) {
    this.numReplicas = numReplicas;
    return this;
  }

  public CreateOptions setNumTagsTablets(int numTagsTablets) {
    Preconditions.checkState(numTagsTabletsPerTabletServer != 0,
                             "tags table tablet options already set");
    this.numTagsTablets = numTagsTablets;
    return this;
  }

  public CreateOptions setNumTagsTabletsPerTabletServer(double numTagsTabletsPerTabletServer) {
    Preconditions.checkState(numTagsTablets != 0,
                             "tags table tablet options already set");
    this.numTagsTabletsPerTabletServer = numTagsTabletsPerTabletServer;
    return this;
  }

  public CreateOptions setNumTagsetsTablets(int numTagsetsTablets) {
    Preconditions.checkState(numTagsetsTabletsPerTabletServer != 0,
                             "tagsets table tablet options already set");
    this.numTagsetsTablets = numTagsetsTablets;
    return this;
  }

  public CreateOptions setNumTagsetsTabletsPerTabletServer(double numTagsetsTabletsPerTabletServer) {
    Preconditions.checkState(numTagsetsTablets != 0,
                             "tagsets table tablet options already set");
    this.numTagsetsTabletsPerTabletServer = numTagsetsTabletsPerTabletServer;
    return this;
  }

  public CreateOptions setNumMetricsHashBuckets(int numMetricsHashBuckets) {
    Preconditions.checkState(numMetricsHashBucketsPerTabletServer != 0,
                             "metrics table tablet options already set");
    this.numMetricsHashBuckets = numMetricsHashBuckets;
    return this;
  }

  public CreateOptions setNumMetricsHashBucketsPerTabletServer(double numMetricsHashBucketsPerTabletServer) {
    Preconditions.checkState(numMetricsHashBuckets != 0,
                             "metrics table tablet options already set");
    this.numMetricsHashBucketsPerTabletServer = numMetricsHashBucketsPerTabletServer;
    return this;
  }

  public CreateOptions setMetricsRangeSplits(List<Long> splits) {
    metricsSplits = splits;
    return this;
  }

  public int getNumReplicas() {
    return numReplicas;
  }

  int getNumTagsTablets(int numTabletServers) {
    if (numTagsTablets != 0) {
      return numTagsTablets;
    } else {
      return Math.max(1, (int) (numTabletServers * numTagsTabletsPerTabletServer));
    }
  }

  int getNumTagsetsTablets(int numTabletServers) {
    if (numTagsetsTablets != 0) {
      return numTagsetsTablets;
    } else {
      return Math.max(1, (int) (numTabletServers * numTagsetsTabletsPerTabletServer));
    }
  }

  int getNumMetricsHashBuckets(int numTabletServers) {
    if (numMetricsHashBuckets != 0) {
      return numMetricsHashBuckets;
    } else {
      return Math.max(1, (int) (numTabletServers * numMetricsHashBucketsPerTabletServer));
    }
  }

  List<Long> getMetricsSplits() {
    return metricsSplits;
  }
}
