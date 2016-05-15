package org.kududb.ts.ktsd;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.util.SortedMap;
import java.util.concurrent.CountDownLatch;

import org.LatencyUtils.LatencyStats;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.kududb.ts.core.Datapoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PutSeries implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(PutSeries.class);

  private final URI url;
  private final String metric;
  private final SortedMap<String, String> tags;
  private final DatapointGenerator datapoints;
  private final LatencyStats stats;
  private final CloseableHttpClient client;
  private final ObjectMapper mapper;
  private final CountDownLatch latch;

  public PutSeries(URI url,
                   String metric,
                   SortedMap<String, String> tags,
                   DatapointGenerator datapoints,
                   LatencyStats stats,
                   CloseableHttpClient client,
                   ObjectMapper mapper,
                   CountDownLatch latch) {
    this.url = url;
    this.metric = metric;
    this.tags = tags;
    this.datapoints = datapoints;
    this.stats = stats;
    this.client = client;
    this.mapper = mapper;
    this.latch = latch;
  }

  @Override
  public void run() {
    try {
      while (datapoints.hasNext()) {
        Datapoint dp = datapoints.next();
        long start = System.nanoTime();

        HttpPost post = new HttpPost(url);
        try {
          post.setEntity(new StringEntity(
              mapper.writeValueAsString(new PutResource.Datapoint(metric, tags, dp.getTime(), dp.getValue())),
              ContentType.APPLICATION_JSON
          ));

          try (CloseableHttpResponse response = client.execute(post)) {
            if (response.getStatusLine().getStatusCode() < 200 ||
                response.getStatusLine().getStatusCode() >= 300) {
              throw new RuntimeException(response.toString());
            }
            stats.recordLatency(System.nanoTime() - start);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    } finally {
      latch.countDown();
    }
  }
}
