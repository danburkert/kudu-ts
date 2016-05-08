package org.kududb.ts.ktsd;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.hibernate.validator.constraints.NotEmpty;
import org.kududb.client.RowError;
import org.kududb.client.RowErrorsAndOverflowStatus;
import org.kududb.client.SessionConfiguration;
import org.kududb.ts.core.KuduTS;
import org.kududb.ts.core.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;

@Path("/put")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class PutResource {
  private static final Logger LOG = LoggerFactory.getLogger(PutResource.class);

  private final KuduTS ts;
  private final ObjectMapper mapper;

  public PutResource(KuduTS ts, ObjectMapper mapper) {
    this.ts = ts;
    this.mapper = mapper;
  }

  @POST
  @Timed
  public Response put(@QueryParam("summary") @DefaultValue("false") BooleanFlag summary,
                      @QueryParam("details") @DefaultValue("false") BooleanFlag details,
                      @QueryParam("sync") @DefaultValue("false") BooleanFlag sync,
                      @QueryParam("sync_timeout") @DefaultValue("0") IntParam sync_timeout,
                      JsonNode body) throws Exception {
    LOG.trace("put; summary: {}, details: {}, sync: {}, sync_timeout: {}, body: {}",
              summary, details, sync, sync_timeout, body);

    WriteBatch batch = ts.writeBatch();
    batch.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    if (sync_timeout.get() > 0) batch.setTimeoutMillis(sync_timeout.get());

    int datapoints = 0;
    List<Error> errors = new ArrayList<>();
    Iterator<JsonNode> nodes;
    if (body.isArray()) {
      nodes = body.elements();
    } else {
      nodes = Iterators.singletonIterator(body);
    }

    while (nodes.hasNext()) {
      datapoints++;
      JsonNode node = nodes.next();
      try {
        Datapoint datapoint = mapper.treeToValue(node, Datapoint.class);
        batch.writeDatapoint(datapoint.getMetric(),
                             datapoint.getTags(),
                             datapoint.getTimestamp(),
                             datapoint.getValue());
      } catch (JsonProcessingException e) {
        errors.add(new Error(node, e.getMessage()));
      }
    }

    batch.flush();
    RowErrorsAndOverflowStatus batchErrors = batch.getPendingErrors();
    for (RowError rowError : batchErrors.getRowErrors()) {
      errors.add(new Error(null, rowError.getErrorStatus().toString()));
    }

    if (errors.isEmpty()) {
      LOG.debug("put {} datapoints: {}", datapoints, body);
      return Response.noContent().build();
    } else {
      LOG.error("failed to write {} of {} body: {}", errors.size(), datapoints, errors);
      if (details.get()) {
        Detail detail = new Detail(errors, errors.size(), datapoints - errors.size());
        return Response.status(Response.Status.BAD_REQUEST).entity(detail).build();
      } else if (summary.get()) {
        Summary s = new Summary(errors.size(), datapoints - errors.size());
        return Response.status(Response.Status.BAD_REQUEST).entity(s).build();
      } else {
        return Response.status(Response.Status.BAD_REQUEST).build();
      }
    }
  }

  private final static class Datapoint {
    @NotEmpty
    private final String metric;
    @NotEmpty
    private final SortedMap<String, String> tags;
    private final long timestamp;
    private final double value;

    @JsonCreator
    public Datapoint(@JsonProperty("metric") String metric,
                     @JsonProperty("tags") SortedMap<String, String> tags,
                     @JsonProperty("timestamp") long timestamp,
                     @JsonProperty("value") double value) {
      this.metric = metric;
      this.tags = tags;
      this.timestamp = timestamp;
      this.value = value;
    }

    public String getMetric() {
      return metric;
    }

    public SortedMap<String, String> getTags() {
      return tags;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public double getValue() {
      return value;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
                        .add("metric", metric)
                        .add("tags", tags)
                        .add("timestamp", timestamp)
                        .add("value", value)
                        .toString();
    }
  }

  private static class Error {
    private final JsonNode datapoint;
    private final String error;

    public Error(JsonNode datapoint, String error) {
      this.datapoint = datapoint;
      this.error = error;
    }

    @JsonProperty("datapoint")
    public JsonNode getDatapoint() {
      return datapoint;
    }

    @JsonProperty("error")
    public String getError() {
      return error;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
                        .add("datapoint", datapoint)
                        .add("error", error)
                        .toString();
    }
  }

  private static class Detail {

    private final List<Error> errors;
    private final int failed;
    private final int success;

    public Detail(List<Error> errors, int failed, int success) {
      this.errors = errors;
      this.failed = failed;
      this.success = success;
    }

    @JsonProperty("errors")
    public List<Error> getErrors() {
      return errors;
    }

    @JsonProperty("failed")
    public int getFailed() {
      return failed;
    }

    @JsonProperty("success")
    public int getSuccess() {
      return success;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
                        .add("errors", errors)
                        .add("failed", failed)
                        .add("success", success)
                        .toString();
    }
  }

  private static class Summary {
    private final int failed;
    private final int success;

    public Summary(int failed, int success) {
      this.failed = failed;
      this.success = success;
    }

    @JsonProperty("failed")
    public int getFailed() {
      return failed;
    }

    @JsonProperty("success")
    public int getSuccess() {
      return success;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
                        .add("failed", failed)
                        .add("success", success)
                        .toString();
    }
  }
}
