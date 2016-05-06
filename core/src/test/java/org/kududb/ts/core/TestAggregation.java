package org.kududb.ts.core;

import static junit.framework.TestCase.assertEquals;

import com.google.common.collect.ImmutableList;

import org.junit.Test;
import org.kududb.ts.core.Aggregators;
import org.kududb.ts.core.Datapoints;
import org.kududb.ts.core.DoubleVec;
import org.kududb.ts.core.IntVec;
import org.kududb.ts.core.LongVec;

public class TestAggregation {

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
      assertEquals(a.getValue(i), b.getValue(i), 1e-9);
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
  public void testMaxAggregation() throws Exception {
    datapointsEqual(datapoints(times(99), values(99.9)),
                    Datapoints.aggregate(ImmutableList.of(datapoints(times(99), values(99.9))),
                                         Aggregators.max()));

    datapointsEqual(datapoints(times(99, 100), values(99.9, 100.1)),
                    Datapoints.aggregate(ImmutableList.of(datapoints(times(99, 100), values(99.9, 100.1))),
                                         Aggregators.max()));

    // Completely overlapping
    datapointsEqual(datapoints(times(0, 1000, 3333), values(10.0, Double.POSITIVE_INFINITY, 100.5)),
                    Datapoints.aggregate(ImmutableList.of(
                        datapoints(times(0, 1000, 3333), values(0.0, Double.POSITIVE_INFINITY, 100.5)),
                        datapoints(times(0, 1000, 3333), values(10.0, -3000.5, 100.5))),
                                         Aggregators.max()));

    // Partially overlapping
    datapointsEqual(datapoints(times(0, 500, 1000, 3333), values(10.0, Double.POSITIVE_INFINITY, -3000.5, 100.5)),
                    Datapoints.aggregate(ImmutableList.of(
                        datapoints(times(0, 500, 3333), values(0.0, Double.POSITIVE_INFINITY, 100.5)),
                        datapoints(times(0, 1000, 3333), values(10.0, -3000.5, 100.5))),
                                         Aggregators.max()));

    // No points in common
    datapointsEqual(datapoints(times(0, 500, 1000, 3333), values(0.0, 10.0, 100.5, -3000.5)),
                    Datapoints.aggregate(ImmutableList.of(
                        datapoints(times(0, 1000), values(0.0, 100.5)),
                        datapoints(times(500, 3333), values(10.0, -3000.5))),
                                         Aggregators.max()));

    // Disjoint
    datapointsEqual(datapoints(times(0, 500, 1000, 3333), values(0.0, 100.5, 10.0, -3000.5)),
                    Datapoints.aggregate(ImmutableList.of(
                        datapoints(times(0, 500), values(0.0, 100.5)),
                        datapoints(times(1000, 3333), values(10.0, -3000.5))),
                                         Aggregators.max()));
  }

  @Test
  public void testMinAggregation() throws Exception {
    datapointsEqual(datapoints(times(99), values(99.9)),
                    Datapoints.aggregate(ImmutableList.of(datapoints(times(99), values(99.9))),
                                         Aggregators.min()));

    datapointsEqual(datapoints(times(99, 100), values(99.9, 100.1)),
                    Datapoints.aggregate(ImmutableList.of(datapoints(times(99, 100), values(99.9, 100.1))),
                                         Aggregators.min()));

    // Completely overlapping
    datapointsEqual(datapoints(times(0, 1000, 3333), values(-10.0, Double.NEGATIVE_INFINITY, 100.5)),
                    Datapoints.aggregate(ImmutableList.of(
                        datapoints(times(0, 1000, 3333), values(-10.0, Double.NEGATIVE_INFINITY, 100.5)),
                        datapoints(times(0, 1000, 3333), values(10.0, -3000.5, 100.5))),
                                         Aggregators.min()));

    // Partially overlapping
    datapointsEqual(datapoints(times(0, 500, 1000, 3333), values(-10.0, Double.NEGATIVE_INFINITY, -3000.5, 100.5)),
                    Datapoints.aggregate(ImmutableList.of(
                        datapoints(times(0, 500, 3333), values(-10.0, Double.NEGATIVE_INFINITY, 100.5)),
                        datapoints(times(0, 1000, 3333), values(10.0, -3000.5, 100.5))),
                                         Aggregators.min()));

    // No points in common
    datapointsEqual(datapoints(times(0, 500, 1000, 3333), values(0.0, 10.0, 100.5, -3000.5)),
                    Datapoints.aggregate(ImmutableList.of(
                        datapoints(times(0, 1000), values(0.0, 100.5)),
                        datapoints(times(500, 3333), values(10.0, -3000.5))),
                                         Aggregators.min()));

    // Disjoint
    datapointsEqual(datapoints(times(0, 500, 1000, 3333), values(0.0, 100.5, 10.0, -3000.5)),
                    Datapoints.aggregate(ImmutableList.of(
                        datapoints(times(0, 500), values(0.0, 100.5)),
                        datapoints(times(1000, 3333), values(10.0, -3000.5))),
                                         Aggregators.min()));
  }

  @Test
  public void testMeanAggregation() throws Exception {
    datapointsEqual(datapoints(times(99), values(99.9)),
                    Datapoints.aggregate(ImmutableList.of(datapoints(times(99), values(99.9))),
                                         Aggregators.mean()));

    datapointsEqual(datapoints(times(99, 100), values(99.9, 100.1)),
                    Datapoints.aggregate(ImmutableList.of(datapoints(times(99, 100), values(99.9, 100.1))),
                                         Aggregators.mean()));

    // Completely overlapping
    datapointsEqual(datapoints(times(0, 1000, 3333), values(2.5, Double.NEGATIVE_INFINITY, 100.5)),
                    Datapoints.aggregate(ImmutableList.of(
                        datapoints(times(0, 1000, 3333), values(-5.0, Double.NEGATIVE_INFINITY, 100.5)),
                        datapoints(times(0, 1000, 3333), values(10.0, -3000.5, 100.5))),
                                         Aggregators.mean()));

    // Partially overlapping
    datapointsEqual(datapoints(times(0, 500, 1000, 3333), values(2.5, Double.POSITIVE_INFINITY, -3000.5, 100.5)),
                    Datapoints.aggregate(ImmutableList.of(
                        datapoints(times(0, 500, 3333), values(-5.0, Double.POSITIVE_INFINITY, 100.5)),
                        datapoints(times(0, 1000, 3333), values(10.0, -3000.5, 100.5))),
                                         Aggregators.mean()));

    // No points in common
    datapointsEqual(datapoints(times(0, 500, 1000, 3333), values(0.0, 10.0, 100.5, -3000.5)),
                    Datapoints.aggregate(ImmutableList.of(
                        datapoints(times(0, 1000), values(0.0, 100.5)),
                        datapoints(times(500, 3333), values(10.0, -3000.5))),
                                         Aggregators.mean()));

    // Disjoint
    datapointsEqual(datapoints(times(0, 500, 1000, 3333), values(0.0, 100.5, 10.0, -3000.5)),
                    Datapoints.aggregate(ImmutableList.of(
                        datapoints(times(0, 500), values(0.0, 100.5)),
                        datapoints(times(1000, 3333), values(10.0, -3000.5))),
                                         Aggregators.mean()));
  }
}
