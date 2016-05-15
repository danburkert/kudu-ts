package org.kududb.ts.ktsd;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.Application;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.client.HttpClientBuilder;
import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;

public class PutBench extends EnvironmentCommand<KTSDConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(PutBench.class);

  public PutBench(Application<KTSDConfiguration> application) {
    super(application, "put-bench", "Benchmark KTSD puts");
  }

  protected void run(Environment environment,
                     Namespace namespace,
                     KTSDConfiguration config) throws Exception {

    final CloseableHttpClient client =
        new HttpClientBuilder(environment).using(config.getBench().getHttpClient())
                                          .build(getName());

    long start = config.getBench().getStart();
    long end = config.getBench().getEnd();
    if (end == 0) end = System.currentTimeMillis();
    if (start == 0) start = end - TimeUnit.HOURS.toMillis(1);
    if (start < 0) start = end - start;

    URI uri = new URIBuilder()
        .setScheme("http")
        .setHost(config.getBench().getKtsdHost())
        .setPort(config.getBench().getKtsdPort())
        .setPath("/api/put")
        .setParameter("summary", "true")
        .build();

    List<SortedMap<String, String>> tags = new ArrayList<>();
    tags.add(new TreeMap<String, String>());
    for (Map.Entry<String, List<String>> tag : config.getBench().getTags().entrySet()) {
      List<SortedMap<String, String>> original = tags;
      tags = new ArrayList<>();
      for (String value : tag.getValue()) {
        for (SortedMap<String, String> ts : original) {
          SortedMap<String, String> copy = new TreeMap<>(ts);
          copy.put(tag.getKey(), value);
          tags.add(copy);
        }
      }
    }

    CountDownLatch latch = new CountDownLatch(config.getBench().getMetrics().size() * tags.size());
    LatencyStats stats = new LatencyStats();

    List<Thread> threads = new ArrayList<>();
    for (String metric : config.getBench().getMetrics()) {
      for (SortedMap<String, String> tagset : tags) {
        DatapointGenerator datapoints = new DatapointGenerator(threads.size(), start, end,
                                                               config.getBench().getSampleFrequency());
        threads.add(new Thread(new PutSeries(uri, metric, tagset, datapoints, stats,
                                             client, environment.getObjectMapper(), latch)));
      }
    }

    for (Thread thread : threads) {
      thread.start();
    }

    Histogram hist = stats.getIntervalHistogram();
    while (!latch.await(10, TimeUnit.SECONDS)) {
      Histogram latest = stats.getIntervalHistogram();
      hist.add(latest);

      LOG.info("Progress:");
      LOG.info("puts: {}/{}", latest.getTotalCount(), hist.getTotalCount());
      LOG.info("mean latency: {}/{}", TimeUnit.NANOSECONDS.toMillis((long) latest.getMean()),
                                      TimeUnit.NANOSECONDS.toMillis((long) hist.getMean()));
      LOG.info("min: {}/{}", TimeUnit.NANOSECONDS.toMillis(latest.getMinValue()),
                             TimeUnit.NANOSECONDS.toMillis(hist.getMinValue()));
      LOG.info("max: {}/{}", TimeUnit.NANOSECONDS.toMillis(latest.getMaxValue()),
                             TimeUnit.NANOSECONDS.toMillis(hist.getMaxValue()));
      LOG.info("p50: {}/{}", TimeUnit.NANOSECONDS.toMillis(latest.getValueAtPercentile(50)),
                             TimeUnit.NANOSECONDS.toMillis(hist.getValueAtPercentile(50)));
      LOG.info("p99: {}/{}", TimeUnit.NANOSECONDS.toMillis(latest.getValueAtPercentile(99)),
                             TimeUnit.NANOSECONDS.toMillis(hist.getValueAtPercentile(99)));
    }

    LOG.info("Benchmark complete");
    LOG.info("puts: {}", hist.getTotalCount());
    LOG.info("mean latency: {}", TimeUnit.NANOSECONDS.toMillis((long) hist.getMean()));
    LOG.info("stddev: {}", TimeUnit.NANOSECONDS.toMillis((long) hist.getStdDeviation()));
    LOG.info("min: {}", TimeUnit.NANOSECONDS.toMillis(hist.getMinValue()));
    LOG.info("max: {}", TimeUnit.NANOSECONDS.toMillis(hist.getMaxValue()));
    LOG.info("p50: {}", TimeUnit.NANOSECONDS.toMillis(hist.getValueAtPercentile(50)));
    LOG.info("p90: {}", TimeUnit.NANOSECONDS.toMillis(hist.getValueAtPercentile(90)));
    LOG.info("p95: {}", TimeUnit.NANOSECONDS.toMillis(hist.getValueAtPercentile(95)));
    LOG.info("p99: {}", TimeUnit.NANOSECONDS.toMillis(hist.getValueAtPercentile(99)));
    LOG.info("p999: {}", TimeUnit.NANOSECONDS.toMillis(hist.getValueAtPercentile(99.9)));
  }
}
