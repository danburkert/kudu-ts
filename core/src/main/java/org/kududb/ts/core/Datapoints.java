package org.kududb.ts.core;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.primitives.Longs;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.concurrent.NotThreadSafe;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Unstable
@NotThreadSafe
public class Datapoints implements Iterable<Datapoint> {

  private final String metric;
  private final IntVec tagsetIDs;
  private final LongVec times;
  private final DoubleVec values;

  Datapoints(String metric, IntVec tagsetIDs, LongVec times, DoubleVec values) {
    Preconditions.checkArgument(tagsetIDs.len() > 0);
    Preconditions.checkArgument(times.len() == values.len());
    this.metric = metric;
    this.tagsetIDs = tagsetIDs;
    this.times = times;
    this.values = values;
  }

  /**
   * Returns the name of the metric.
   */
  String getMetric() {
    return metric;
  }

  /**
   * Returns the tagset IDs of the datapoints.
   * Package private because tagset IDs are an internal implementation detail.
   * @return the tagset IDs
   */
  IntVec getTagsetIDs() {
    return tagsetIDs;
  }

  /**
   * Returns the time of the {@link Datapoint} at the provided index.
   * @param index of the {@code Datapoint} whose time to return
   * @return the time of the {@code Datapoint} at the provided index
   */
  long getTime(int index) {
    return times.get(index);
  }

  /**
   * Returns the value of the {@link Datapoint} at the provided index.
   * @param index of the {@code Datapoint} whose value to return
   * @return the value of the {@code Datapoint} at the provided index
   */
  double getValue(int index) {
    return values.get(index);
  }

  /**
   * Returns the number of data points.
   */
  int size() {
    return times.len();
  }

  /**
   * Returns an iterator over the {@link Datapoint}s of this {@code Datapoints}.
   * The iterator reuses the same {@code Datapoint} instance to save allocations,
   * see {@link Iterator} for details.
   */
  public Datapoints.Iterator iterator() {
    return new Iterator();
  }

  /**
   * Returns the vector of datapoint times in microseconds.
   * Callers should not modify the vector.
   * Package private because {@link LongVec} is internal.
   *
   * @return the vector of datapoint times
   */
  LongVec getTimes() {
    return times;
  }

  /**
   * Returns the vector of datapoint values.
   * Callers should not modify the vector.
   * Package private because {@link DoubleVec} is internal.
   *
   * @return the vector of datapoint times
   */
  DoubleVec getValues() {
    return values;
  }

  /**
   * Aggregate multiple {@code Datapoints}. The {@code Datapoints} must have the
   * same {@code metric}, but may come from different time ranges or tagsets.
   * This method takes ownership of the provided {@code series}, they should not
   * be used after calling this.
   * @param series the list of series to aggregate
   * @param aggregator the aggregator
   * @return an aggregated series
   */
  public static Datapoints aggregate(List<Datapoints> series, Aggregator aggregator) {
    final class IteratorComparator implements Comparator<Iterator> {
      @Override
      public int compare(Iterator a, Iterator b) {
        return Longs.compare(a.peek().getTime(), b.peek().getTime());
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper(this).toString();
      }
    }

    if (series.isEmpty()) throw new IllegalArgumentException("no series to aggregate");

    PriorityQueue<Iterator> iterators =
        new PriorityQueue<>(series.size(), new IteratorComparator());

    String metric = series.get(0).getMetric();
    IntVec tagsetIDs = IntVec.withCapacity(series.size());

    for (Datapoints s : series) {
      if (s.size() == 0) throw new IllegalArgumentException("empty series");
      iterators.add(s.iterator());
      tagsetIDs.concat(s.tagsetIDs);
      if (!metric.equals(s.getMetric())) {
        throw new IllegalArgumentException("unable to aggregate datapoints from different metrics");
      }
    }

    LongVec times = LongVec.create();
    DoubleVec values = DoubleVec.create();

    while (!iterators.isEmpty()) {
      long time;
      {
        Iterator iter = iterators.remove();
        Datapoint point = iter.next();
        time = point.getTime();
        times.push(time);
        aggregator.addValue(point.getValue());
        if (iter.hasNext()) iterators.add(iter);
      }
      while (!iterators.isEmpty() && iterators.peek().peek().getTime() == time) {
        Iterator i = iterators.remove();
        aggregator.addValue(i.next().getValue());
        if (i.hasNext()) iterators.add(i);
      }
      values.push(aggregator.aggregatedValue());
    }

    return new Datapoints(metric, tagsetIDs, times, values);
  }

  public static Datapoints aggregate(List<Datapoints> series,
                                     Aggregator aggregator,
                                     Interpolators.Interpolator interpolator)  {
    if (series.isEmpty()) throw new IllegalArgumentException("no series to aggregate");
    String metric = series.get(0).getMetric();
    IntVec tagsetIDs = IntVec.withCapacity(series.size());

    List<Interpolation> active = new ArrayList<>(series.size());
    List<Interpolation> inactive = new ArrayList<>(series.size());

    LongVec times = LongVec.withCapacity(0);
    for (Datapoints s : series) {
      if (s.size() == 0) throw new IllegalArgumentException("empty series");
      tagsetIDs.concat(s.tagsetIDs);
      if (!metric.equals(s.getMetric())) {
        throw new IllegalArgumentException("unable to aggregate datapoints from different metrics");
      }
      times.merge(s.getTimes());
      inactive.add(interpolator.interpolate(s));
    }

    // TODO: use mergeUnique instead of merge above to get rid of dedup.
    times.dedup();

    DoubleVec values = DoubleVec.withCapacity(times.len());
    long nextMinMaxTime = Long.MIN_VALUE;
    LongVec.Iterator timeIter = times.iterator();
    while (timeIter.hasNext()) {
      long time = timeIter.next();

      if (time >= nextMinMaxTime) {
        nextMinMaxTime = Long.MAX_VALUE;
        { // expire active interpolations, if possible
          java.util.Iterator<Interpolation> iter = active.iterator();
          while (iter.hasNext()) {
            Interpolation next = iter.next();
            long maxTime = next.maxTime();
            if (maxTime < time) {
              iter.remove();
            } else {
              nextMinMaxTime = Math.min(nextMinMaxTime, maxTime);
            }
          }
        }
        { // promote inactive interpolations to active, if possible
          java.util.Iterator<Interpolation> iter = inactive.iterator();
          while (iter.hasNext()) {
            Interpolation next = iter.next();
            if (next.maxTime() < time)
              throw new IllegalStateException("inactive interpolation is expired");
            long minTime = next.minTime();
            if (next.minTime() <= time) {
              iter.remove();
              active.add(next);
              nextMinMaxTime = Math.min(nextMinMaxTime, next.maxTime());
            } else {
              nextMinMaxTime = Math.min(nextMinMaxTime, minTime);
            }
          }
        }
      }

      if (active.isEmpty()) throw new IllegalStateException("no active interpolations");

      for (Interpolation interpolation : active) {
        aggregator.addValue(interpolation.getValue(time));
      }
      values.push(aggregator.aggregatedValue());
    }
    return new Datapoints(metric, tagsetIDs, times, values);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;
    Datapoints that = (Datapoints) other;
    return Objects.equal(metric, that.metric) &&
           Objects.equal(tagsetIDs, that.tagsetIDs) &&
           Objects.equal(times, that.times) &&
           Objects.equal(values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(metric, tagsetIDs, times, values);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("metric", metric)
                      .add("series", tagsetIDs.len())
                      .add("datapoints", times.len())
                      .toString();
  }

  /**
   * A {@link Datapoint} iterator over a {@code Datapoints}. The
   * {@code Datapoint} returned by {@link #next} is reused, so callers should
   * not hold on to it. If the {@code Datapoint}'s time or value needs to
   * be saved between calls to {@link #next}, then the caller is reponsible for
   * copying them.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  @NotThreadSafe
  public class Iterator extends UnmodifiableIterator<Datapoint> implements PeekingIterator<Datapoint> {
    private final Datapoint datapoint = Datapoint.create(0, 0);
    private final LongVec.Iterator timestampIter = times.iterator();
    private final DoubleVec.Iterator valueIter = values.iterator();

    public boolean hasNext() {
      return timestampIter.hasNext();
    }

    @Override
    public Datapoint next() {
      datapoint.setTime(timestampIter.next());
      datapoint.setValue(valueIter.next());
      return datapoint;
    }

    @Override
    public Datapoint peek() {
      datapoint.setTime(timestampIter.peek());
      datapoint.setValue(valueIter.peek());
      return datapoint;
    }

    /**
     * Seek to the first datapoint at or after the provided time in microseconds.
     * @param microseconds the time to seek to
     */
    public void seek(long microseconds) {
      timestampIter.seekToValue(microseconds);
      valueIter.seek(timestampIter.getIndex());
    }
  }
}
