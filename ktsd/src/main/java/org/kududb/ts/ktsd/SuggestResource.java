package org.kududb.ts.ktsd;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.kududb.ts.core.KuduTS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/suggest")
@Produces(MediaType.APPLICATION_JSON)
public class SuggestResource {
  private static final Logger LOG = LoggerFactory.getLogger(SuggestResource.class);

  private final KuduTS ts;

  public SuggestResource(KuduTS ts) {
    this.ts = ts;
  }

  @GET
  @Timed
  public List<String> get(@QueryParam("type") @NotNull String type,
                          @QueryParam("q") String query,
                          @QueryParam("max") @DefaultValue("25") int max) throws Exception {
    LOG.trace("suggest; type: {}, query: {}, max: {}", type, query, max);

    if (type.equals("metrics")) {

    } else if (type.equals("tagk")) {

    } else if (type.equals("tagv")) {

    }

    return ImmutableList.of();
  }

  @POST
  @Timed
  public List<String> post(Request request) throws Exception {
    return get(request.type, request.query, request.max);
  }


  private static class Request {

    @NotNull
    private final String type;

    private final String query;

    @DefaultValue("25")
    private final int max;

    @JsonCreator
    public Request(@JsonProperty("type") String type,
                   @JsonProperty("q") String query,
                   @JsonProperty("max") int max) {
      this.type = type;
      this.query = query;
      this.max = max;
    }
  }
}
