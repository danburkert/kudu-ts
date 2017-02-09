package org.kududb.ts.core;

import com.google.common.base.MoreObjects;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kudu.annotations.InterfaceAudience;

/**
 * An {@code Interpolation} wraps a {@link Datapoints} and provides interpolated
 * values for times which do not have a datapoint. The {@code Interpolation} is
 * meant to be used to build a time series in order, so it only allows in-order
 * retrieval of interpolated values.
 */
@InterfaceAudience.Private
@NotThreadSafe
abstract class Interpolation {

  final Datapoints datapoints;

  private Interpolation(Datapoints datapoints) {
    if (datapoints.size() == 0) throw new IllegalArgumentException("empty datapoints");
    this.datapoints = datapoints;
  }

  /**
   * Get the interpolated value at the specified time in microseconds.
   * The provided time must be greater than or equal to {@link #minTime},
   * less than or equal to {@link #maxTime()} and greater than or equal to the
   * previous interpolated time.
   * @param time the time whose interpolated value will be returned
   * @return the interpolated value
   */
  public abstract double getValue(long time);

  /**
   * The inclusive minimum time for this interpolator in microseconds.
   * @return the minimum time in microseconds.
   */
  public abstract long minTime();

  /**
   * The exclusive maximum time for this interpolator in microseconds.
   * @return the maximum time in microseconds.
   */
  public abstract long maxTime();

  /**
   * Interpolation which uses linear interpolation to estimate missingn values.
   *
   * @see <a href="https://en.wikipedia.org/wiki/Linear_interpolation">Linear interpolation</a>
   */
  @InterfaceAudience.Private
  @NotThreadSafe
  static final class Linear extends Interpolation {
    private long t0;
    private long t1;
    private double v0;
    private double v1;

    /** Index of t1/v1 in datapoints */
    private int index;

    public Linear(Datapoints datapoints) {
      super(datapoints);

      t0 = datapoints.getTime(0);
      v0 = datapoints.getValue(0);
      if (datapoints.size() == 1) {
        t1 = t0;
        v1 = v0;
        index = 0;
      } else {
        t1 = datapoints.getTime(1);
        v1 = datapoints.getValue(1);
        index = 1;
      }
    }

    @Override
    public double getValue(long time) {
      if (time < t0) {
        throw new IllegalArgumentException(String.format("time: %s; expected %s or greater",
                                                         time, t0));
      }

      while (time > t1) {
        t0 = t1;
        v0 = v1;
        t1 = datapoints.getTime(++index);
        v1 = datapoints.getValue(index);
      }

      if (time == t0) return v0;
      if (time == t1) return v1;
      return v0 + (v1 - v0) * ((time - t0) / (double) (t1 - t0));
    }

    @Override
    public long minTime() {
      return datapoints.getTime(0);
    }

    @Override
    public long maxTime() {
      return datapoints.getTime(datapoints.size() - 1) + 1;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("datapoints", datapoints).toString();
    }
  }
}