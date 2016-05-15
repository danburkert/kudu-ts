package org.kududb.ts.ktsd;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotEmpty;

import io.dropwizard.Configuration;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.jersey.params.IntParam;

public class KTSDConfiguration extends Configuration {

  @NotEmpty
  private final List<String> kuduMasterAddresses;

  @NotEmpty
  private final String kuduTSInstance;

  private final int kuduTSReplicas;

  @Valid
  BenchConfiguration bench;


  @JsonCreator
  public KTSDConfiguration(@JsonProperty("kuduMasterAddresses") List<String> kuduMasterAddresses,
                           @JsonProperty("kuduTSInstance") String kuduTSInstance,
                           @JsonProperty("kuduTSReplicas") int replicas,
                           @JsonProperty("bench") BenchConfiguration bench) {
    this.kuduMasterAddresses = kuduMasterAddresses;
    this.kuduTSInstance = kuduTSInstance;
    this.kuduTSReplicas = replicas;
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
  public int getKuduTSReplicas() {
    return kuduTSReplicas;
  }

  @JsonProperty
  public BenchConfiguration getBench() {
    return bench;
  }
}
