package org.kududb.ts.ktsd;

import com.google.common.collect.UnmodifiableIterator;

import java.util.Random;

import org.kududb.ts.core.Datapoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatapointGenerator extends UnmodifiableIterator<Datapoint> {
  private static final Logger LOG = LoggerFactory.getLogger(DatapointGenerator.class);

  private final Random rand;
  private final long start;
  private final long end;
  private final double frequency;
  private final double variance;

  private int nextSample;
  private long nextTime;
  private double nextValue;

  public DatapointGenerator(long seed,
                            long start,
                            long end,
                            double frequency) {
    this.rand = new Random(seed);
    this.start = start;
    this.end = end;
    this.frequency = frequency;
    this.nextTime = start;

    nextSample = 0;
    nextValue = Math.log(1-rand.nextDouble())/(-0.01);
    variance = Math.sqrt(nextValue);
    LOG.info("start time: {}", start);
    LOG.info("end time: {}", end);
  }

  @Override
  public boolean hasNext() {
    return nextTime <= end;
  }

  @Override
  public Datapoint next() {
    Datapoint dp = Datapoint.create(nextTime, nextValue);
    nextTime = (long) (start + 1000 * (++nextSample) / frequency);
    nextValue += Math.max(0, rand.nextDouble() * variance);
    return dp;
  }
}
