package org.kududb.ts.ktsd;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;

import org.hibernate.validator.constraints.NotEmpty;

import io.dropwizard.Configuration;
import io.dropwizard.client.HttpClientConfiguration;

public class BenchConfiguration extends Configuration {

  @NotEmpty
  @NotNull
  private List<String> metrics;
  @NotEmpty
  @NotNull
  private Map<String, List<String>> tags;
  private double sampleFrequency;
  private long start;
  private long end;

  private String ktsdHost;
  private int ktsdPort;
  private boolean sync;

  @Valid
  @NotNull
  @JsonProperty
  private HttpClientConfiguration httpClient = new HttpClientConfiguration();

  @JsonCreator
  public BenchConfiguration(@JsonProperty("metrics") List<String> metrics,
                            @JsonProperty("tags") Map<String, List<String>> tags,
                            @JsonProperty("sampleFrequency") @DefaultValue("1.0") double sampleFrequency,
                            @JsonProperty("start") @DefaultValue("0") long start,
                            @JsonProperty("end") @DefaultValue("0") long end,
                            @JsonProperty("ktsdHost") @DefaultValue("localhost") String host,
                            @JsonProperty("ktsdPort") @DefaultValue("4242") int port,
                            @JsonProperty("sync") @DefaultValue("true") boolean sync,
                            @JsonProperty("httpClient") HttpClientConfiguration httpClient) {
    this.metrics = metrics;
    this.tags = tags;
    this.sampleFrequency = sampleFrequency;
    this.start = start;
    this.end = end;
    this.ktsdHost = host;
    this.ktsdPort = port;
    this.sync = sync;
    this.httpClient = httpClient;
  }

  @JsonProperty
  public List<String> getMetrics() {
    return metrics;
  }

  @JsonProperty
  public Map<String, List<String>> getTags() {
    return tags;
  }

  @JsonProperty
  public double getSampleFrequency() {
    return sampleFrequency;
  }

  @JsonProperty
  public long getStart() {
    return start;
  }

  @JsonProperty
  public long getEnd() {
    return end;
  }

  @JsonProperty
  public String getKtsdHost() {
    return ktsdHost;
  }

  @JsonProperty
  public int getKtsdPort() {
    return ktsdPort;
  }

  @JsonProperty
  public boolean isSync() {
    return sync;
  }

  @JsonProperty
  public HttpClientConfiguration getHttpClient() {
    return httpClient;
  }
}
