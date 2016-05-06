package org.kududb.ts.core;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;
import org.kududb.ts.core.Interpolators.Interpolator;

@InterfaceAudience.Public
@InterfaceStability.Evolving
@NotThreadSafe
public class Query {
  private final String metric;
  private final Map<String, String> tags;
  private final Aggregator aggregator;

  private long start = Long.MIN_VALUE;
  private long end = Long.MAX_VALUE;
  private Aggregator downsampler = null;
  private long downsampleInterval = 0;
  private Interpolator interpolator = null;

  private Query(String metric, Map<String, String> tags, Aggregator aggregator) {
    Preconditions.checkNotNull(metric, "query must have a metric");
    Preconditions.checkNotNull(tags, "query must have tags");
    Preconditions.checkNotNull(aggregator, "query must have aggregator");
    Preconditions.checkArgument(!tags.isEmpty(), "query must have at least one tag");

    this.metric = metric;
    this.tags = tags;
    this.aggregator = aggregator;
  }

  /**
   * Create a new query on a KuduTS table.
   * @param metric the metric to query
   * @param tags the set of tags to filter by. Must contain at least one tag
   * @param aggregator a series aggregator
   * @return the KuduTS query
   */
  public static Query create(String metric,
                             Map<String, String> tags,
                             Aggregator aggregator) {
    return new Query(metric, tags, aggregator);
  }

  /**
   * Sets the inclusive start time of the query in microseconds.
   * @param start time of query in microseconds
   * @return this
   */
  public Query setStart(long start) {
    Preconditions.checkArgument(start < end, "query start time must be less than the end time");
    this.start = start;
    return this;
  }

  /**
   * Sets the exclusive end time of the query in microseconds.
   * @param end time of query in microseconds
   * @return this
   */
  public Query setEnd(long end) {
    Preconditions.checkArgument(start < end, "query end time must be greater than the start time");
    this.end = end;
    return this;
  }

  /**
   * Sets the downsampler and downsample interval.
   * @param downsampler the aggregation method to use when downsampling
   * @param interval time length of each downsample
   * @return this
   */
  public Query setDownsampler(Aggregator downsampler, long interval) {
    Preconditions.checkArgument(interval > 0, "query downsample interval must be greater than 0");
    this.downsampler = Preconditions.checkNotNull(downsampler);
    this.downsampleInterval = interval;
    return this;
  }

  /**
   * Sets the interpolator.
   * @param interpolator method to interpolate missing results with
   * @return this
   */
  public Query setInterpolator(Interpolator interpolator) {
    this.interpolator = Preconditions.checkNotNull(interpolator);
    return this;
  }

  /**
   * Returns the metric being queried.
   * @return the metric
   */
  public String getMetric() {
    return metric;
  }

  /**
   * Returns the inclusive start time of the query in microseconds.
   * @return the start time
   */
  public long getStart() {
    return start;
  }

  /**
   * Returns the exclusive end time of the query in microseconds.
   * @return the end time
   */
  public long getEnd() {
    return end;
  }

  /**
   * Returns the set of tags which the query filters by.
   * @return the query tags
   */
  public Map<String, String> getTags() {
    return tags;
  }

  public Aggregator getDownsampler() {
    return downsampler;
  }

  public long getDownsampleInterval() {
    return downsampleInterval;
  }

  public Interpolators.Interpolator getInterpolator() {
    return interpolator;
  }

  public Aggregator getAggregator() {
    return aggregator;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("metric", metric)
                      .add("tags", tags)
                      .add("start", start)
                      .add("end", end)
                      .add("downsampler", downsampler)
                      .add("downsampleInterval", downsampleInterval)
                      .add("interpolator", interpolator)
                      .add("aggregator", aggregator)
                      .toString();
  }
}
