package org.kududb.ts.ktsd;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import org.hibernate.validator.constraints.NotEmpty;

import io.dropwizard.Configuration;

public class KTSDConfiguration extends Configuration {

  @NotEmpty
  private final List<String> kuduMasterAddresses;

  @NotEmpty
  private final String kuduTSInstance;

  @JsonCreator
  public KTSDConfiguration(@JsonProperty("kuduMasterAddresses") List<String> kuduMasterAddresses,
                           @JsonProperty("kuduTSInstance") String kuduTSInstance) {
    this.kuduMasterAddresses = kuduMasterAddresses;
    this.kuduTSInstance = kuduTSInstance;
  }

  @JsonProperty
  public List<String> getKuduMasterAddresses() {
    return kuduMasterAddresses;
  }

  @JsonProperty
  public String getKuduTSInstance() {
    return kuduTSInstance;
  }
}
