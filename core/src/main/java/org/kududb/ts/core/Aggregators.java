package org.kududb.ts.core;

import com.google.common.base.MoreObjects;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.annotations.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
@NotThreadSafe
public class Aggregators {

  private Aggregators() {}

  public static Aggregator min() {
    return new Min();
  }

  public static Aggregator max() {
    return new Max();
  }

  public static Aggregator sum() {
    return new Sum();
  }

  /**
   * Returns an {@link Aggregator} that computes the arithmetic mean of a series.
   * @return an arithmetic mean aggregator
   */
  public static Aggregator mean() {
    return new Mean();
  }

  public static Aggregator standardDeviation() {
    return new StandardDeviation();
  }

  private static class Min implements Aggregator {
    private double min = Double.POSITIVE_INFINITY;
    @Override
    public void addValue(double value) {
      min = Math.min(value, min);
    }
    @Override
    public double aggregatedValue() {
      double aggregatedMin = min;
      min = Double.POSITIVE_INFINITY;
      return aggregatedMin;
    }
    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("min", min).toString();
    }
  }

  private static class Max implements Aggregator {
    private double max = Double.NEGATIVE_INFINITY;
    @Override
    public void addValue(double value) {
      max = Math.max(value, max);
    }
    @Override
    public double aggregatedValue() {
      double aggregatedMax = max;
      max = Double.NEGATIVE_INFINITY;
      return aggregatedMax;
    }
    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("max", max).toString();
    }
  }

  private static class Sum implements Aggregator {
    private double sum = 0;
    @Override
    public void addValue(double value) {
      sum += value;
    }
    @Override
    public double aggregatedValue() {
      double aggregatedSum = sum;
      sum = 0;
      return aggregatedSum;
    }
    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("sum", sum).toString();
    }
  }

  /**
   * TODO: this doesn't guard against overflow.
   */
  private static class Mean implements Aggregator {
    private int samples = 0;
    private double sum = 0;
    @Override
    public void addValue(double value) {
      sum += value;
      samples++;
    }
    @Override
    public double aggregatedValue() {
      double aggregatedMean = sum / samples;
      sum = 0;
      samples = 0;
      return aggregatedMean;
    }
    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
                        .add("sum", sum)
                        .add("samples", samples)
                        .toString();
    }
  }

  /** http://www.johndcook.com/blog/standard_deviation */
  private static class StandardDeviation implements Aggregator {
    private int samples = 0;
    double oldM, newM, oldS, newS = 0;

    @Override
    public void addValue(double value) {
      samples++;

      // See Knuth TAOCP vol 2, 3rd edition, page 232
      if (samples == 1) {
        oldM = 0;
        newM = value;
        oldS = 0;
      } else {
        newM = oldM + (value - oldM) / samples;
        newS = oldS + (value - oldM) * (value - newM);

        // set up for next iteration
        oldM = newM;
        oldS = newS;
      }
    }

    @Override
    public double aggregatedValue() {
      double stddev = Math.sqrt(newS / (samples - 1));
      samples = 0;
      return stddev;
    }
    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
                        .add("stddev", Math.sqrt(newS / (samples - 1)))
                        .add("samples", samples)
                        .toString();
    }
  }
}
