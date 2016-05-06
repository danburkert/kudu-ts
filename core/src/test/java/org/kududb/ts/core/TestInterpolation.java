package org.kududb.ts.core;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;

import org.junit.Test;
import org.kududb.ts.core.Aggregators;
import org.kududb.ts.core.Datapoints;
import org.kududb.ts.core.DoubleVec;
import org.kududb.ts.core.IntVec;
import org.kududb.ts.core.Interpolators;
import org.kududb.ts.core.LongVec;

public class TestInterpolation {
  /**
   * Tests that two {@link Datapoints} are equal, except for the series IDs.
   * @param a the first datapoints to compare
   * @param b the second datapoints to compare
   */
  private static void datapointsEqual(Datapoints a, Datapoints b) {
    assertEquals(a.getMetric(), b.getMetric());
    assertEquals(a.getTimes(), b.getTimes());
    assertEquals(a.getValues().len(), b.getValues().len());

    for (int i = 0; i < a.getValues().len(); i++) {
      assertEquals(a.getValue(i), b.getValue(i), 1e-3);
    }
  }

  private static long[] times(long... times) {
    return times;
  }

  private static double[] values(double... values) {
    return values;
  }

  private static Datapoints datapoints(long[] times, double[] values) {
    return new Datapoints("m", IntVec.wrap(new int[] { 0 }),
                          LongVec.wrap(times), DoubleVec.wrap(values));
  }

  @Test
  public void testLinearInterpolation() throws Exception {
    Interpolators.Interpolator linear = Interpolators.linear();

    datapointsEqual(datapoints(times(99), values(99.9)),
                    Datapoints.aggregate(ImmutableList.of(datapoints(times(99), values(99.9))),
                                         Aggregators.max(),
                                         linear));

    datapointsEqual(datapoints(times(99, 100), values(99.9, 100.1)),
                    Datapoints.aggregate(ImmutableList.of(datapoints(times(99, 100), values(99.9, 100.1))),
                                         Aggregators.max(),
                                         linear));

    // Completely overlapping
    datapointsEqual(datapoints(times(0, 1000, 3333), values(5.0, -1475, 100.5)),
                    Datapoints.aggregate(ImmutableList.of(
                        datapoints(times(0, 1000, 3333), values(0.0, 50.5, 100.5)),
                        datapoints(times(0, 1000, 3333), values(10.0, -3000.5, 100.5))),
                                         Aggregators.mean(),
                                         linear));
    // Partially overlapping
    datapointsEqual(datapoints(times(0, 500, 1000, 3333), values(5.0, -722.375, -1470.5877, 100.5)),
                    Datapoints.aggregate(ImmutableList.of(
                        datapoints(times(0, 500, 3333), values(0.0, 50.5, 100.5)),
                        datapoints(times(0, 1000, 3333), values(10.0, -3000.5, 100.5))),
                                         Aggregators.mean(),
                                         linear));


    // No points in common
    datapointsEqual(datapoints(times(0, 500, 1000, 1500), values(0.0, 30.0, 57.5, 20)),
                    Datapoints.aggregate(ImmutableList.of(
                        datapoints(times(0, 1000), values(0.0, 100.0)),
                        datapoints(times(500, 1500), values(10.0, 20.0))),
                                         Aggregators.mean(),
                                         linear));

    // Disjoint
    datapointsEqual(datapoints(times(0, 500, 1000, 3333), values(0.0, 100.5, 10.0, -3000.5)),
                    Datapoints.aggregate(ImmutableList.of(
                        datapoints(times(0, 500), values(0.0, 100.5)),
                        datapoints(times(1000, 3333), values(10.0, -3000.5))),
                                         Aggregators.max(),
                                         linear));
  }
}
