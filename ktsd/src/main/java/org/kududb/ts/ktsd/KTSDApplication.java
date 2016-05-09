package org.kududb.ts.ktsd;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class KTSDApplication extends Application<KTSDConfiguration> {

  public static void main(String[] args) throws Exception {
    new KTSDApplication().run(args);
  }

  @Override
  public String getName() {
    return "Kudu TS Daemon";
  }

  @Override
  public void initialize(Bootstrap<KTSDConfiguration> bootstrap) {
    super.initialize(bootstrap);
    bootstrap.addCommand(new PutBench(this));
  }

  @Override
  public void run(KTSDConfiguration configuration,
                  Environment environment) throws Exception {
    ManagedKuduTS ts = new ManagedKuduTS(configuration);
    environment.lifecycle().manage(ts);
    environment.jersey().register(new PutResource(ts.ts(), environment.getObjectMapper()));
    environment.jersey().register(new QueryResource(ts.ts()));
  }
}
