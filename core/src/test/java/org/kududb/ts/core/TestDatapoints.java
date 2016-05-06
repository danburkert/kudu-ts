package org.kududb.ts.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;
import org.kududb.ts.core.Datapoints;
import org.kududb.ts.core.DoubleVec;
import org.kududb.ts.core.IntVec;
import org.kududb.ts.core.LongVec;

public class TestDatapoints {

  @Test
  public void testDatapointAccessors() throws Exception {
    int[] tagsetIDs = { 0 };
    long[] times = { 0, 1, 10, 100, 101, 102, 104, 105, 200, 10000 };
    double[] values = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

    Datapoints datapoints = new Datapoints("test",
                                           IntVec.wrap(tagsetIDs),
                                           LongVec.wrap(times),
                                           DoubleVec.wrap(values));

    assertEquals(10, datapoints.size());

    for (int i = 0; i < 10; i++) {
      assertEquals(times[i], datapoints.getTime(i));
      assertEquals(values[i], datapoints.getValue(i), 0);
    }
  }

  @Test
  public void testDatapointIteratorSeek() throws Exception {
    int[] tagsetIDs = { 0 };
    long[] times = { 10, 100, 101, 102, 104, 105, 200, 10000 };
    double[] values = { 0, 1, 2, 3, 4, 5, 6, 7 };

    Datapoints datapoints = new Datapoints("test",
                                           IntVec.wrap(tagsetIDs),
                                           LongVec.wrap(times),
                                           DoubleVec.wrap(values));
    Datapoints.Iterator iter = datapoints.iterator();

    iter.seek(0);
    assertEquals(10, iter.next().getTime());

    iter.seek(9);
    assertEquals(10, iter.next().getTime());

    iter.seek(10);
    assertEquals(10, iter.next().getTime());

    iter.seek(11);
    assertEquals(100, iter.next().getTime());

    iter.seek(100);
    assertEquals(100, iter.next().getTime());

    iter.seek(50);
    assertEquals(100, iter.next().getTime());

    iter.seek(10000);
    assertEquals(10000, iter.next().getTime());

    iter.seek(10001);
    assertFalse(iter.hasNext());
  }
}
