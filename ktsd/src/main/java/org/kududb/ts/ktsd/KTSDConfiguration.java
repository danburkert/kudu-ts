package org.kududb.ts.ktsd;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotEmpty;

import io.dropwizard.Configuration;
import io.dropwizard.client.JerseyClientConfiguration;

public class KTSDConfiguration extends Configuration {

  @NotEmpty
  private final List<String> kuduMasterAddresses;

  @NotEmpty
  private final String kuduTSInstance;

  @Valid
  BenchConfiguration bench;

  @JsonCreator
  public KTSDConfiguration(@JsonProperty("kuduMasterAddresses") List<String> kuduMasterAddresses,
                           @JsonProperty("kuduTSInstance") String kuduTSInstance,
                           @JsonProperty("bench") BenchConfiguration bench) {
    this.kuduMasterAddresses = kuduMasterAddresses;
    this.kuduTSInstance = kuduTSInstance;
    this.bench = bench;
  }

  @JsonProperty
  public List<String> getKuduMasterAddresses() {
    return kuduMasterAddresses;
  }

  @JsonProperty
  public String getKuduTSInstance() {
    return kuduTSInstance;
  }

  @JsonProperty
  public BenchConfiguration getBench() {
    return bench;
  }
}
