package org.kududb.ts.core;

import com.google.common.base.MoreObjects;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.annotations.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Unstable
@NotThreadSafe
public final class Datapoint {
  private long time;
  private double value;

  private Datapoint(long time, double value) {
    this.time = time;
    this.value = value;
  }

  /**
   * Creates a new {@code Datapoint} with the provided time and value.
   * @param microseconds the {@code Datapoint}'s time in microseconds
   * @param value the {@code Datapoint}'s value
   * @return a new {@code Datapoint}
   */
  public static Datapoint create(long microseconds, double value) {
    return new Datapoint(microseconds, value);
  }

  /**
   * Returns the {@code Datapoint}'s time in microseconds.
   * @return the {@code Datapoint}'s time in microseconds
   */
  public long getTime() {
    return time;
  }

  /**
   * Returns the {@code Datapoint}'s value.
   * @return the {@code Datapoint}'s value
   */
  public double getValue() {
    return value;
  }

  /**
   * Sets the time of the {@code Datapoint}.
   * @param microseconds the new time in microseconds
   */
  public void setTime(long microseconds) {
    time = microseconds;
  }

  /**
   * Sets the value of the {@code Datapoint}.
   * @param value the new value
   */
  public void setValue(double value) {
    this.value = value;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("time", time)
                      .add("value", value)
                      .toString();
  }
}
