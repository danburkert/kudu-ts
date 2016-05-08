package org.kududb.ts.ktsd;

import org.kududb.ts.core.CreateOptions;
import org.kududb.ts.core.KuduTS;

import io.dropwizard.lifecycle.Managed;

public class ManagedKuduTS implements Managed {

  private final KuduTS ts;

  public ManagedKuduTS(KTSDConfiguration configuration) throws Exception {
    ts = KuduTS.openOrCreate(configuration.getKuduMasterAddresses(),
                             configuration.getKuduTSInstance(),
                             CreateOptions.defaults());
  }

  public KuduTS ts() {
    return ts;
  }

  @Override
  public void start() {}

  @Override
  public void stop() throws Exception {
    ts.close();
  }
}
