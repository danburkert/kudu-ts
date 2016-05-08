package org.kududb.ts.ktsd;

import io.dropwizard.jersey.params.AbstractParam;

public class BooleanFlag extends AbstractParam<Boolean> {

  public BooleanFlag(String input) {
    super(input);
  }

  @Override
  protected String errorMessage(Exception e) {
    return "Parameter must be \"true\" or \"false\".";
  }

  @Override
  protected Boolean parse(String input) throws Exception {
    if (input.isEmpty()) {
      return Boolean.TRUE;
    } else if ("true".equalsIgnoreCase(input)) {
      return Boolean.TRUE;
    } else if ("false".equalsIgnoreCase(input)) {
      return Boolean.FALSE;
    }
    throw new Exception();
  }
}
