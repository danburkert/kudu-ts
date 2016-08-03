package org.kududb.ts.ktsd;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.kududb.ts.core.Aggregator;
import org.kududb.ts.core.Aggregators;
import org.kududb.ts.core.Datapoint;
import org.kududb.ts.core.Datapoints;
import org.kududb.ts.core.Interpolators;
import org.kududb.ts.core.KuduTS;
import org.kududb.ts.core.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.validation.Validated;

@Path("/query")
@Produces(MediaType.APPLICATION_JSON)
public class QueryResource {
  private static final Logger LOG = LoggerFactory.getLogger(QueryResource.class);
  private static final Pattern INTERPOLATOR_REGEX = Pattern.compile("^([0-9]+)([A-z]+)-([A-z]+)$");

  private final KuduTS ts;
  private final ObjectMapper mapper;

  public QueryResource(KuduTS ts, ObjectMapper mapper) {
    this.ts = ts;
    this.mapper = mapper;
  }

  @POST
  @Timed
  @Consumes(MediaType.APPLICATION_JSON)
  public List<Result> query(@Validated Request request) throws Exception {
    LOG.trace("query: {}", request);

    List<Query> queries = new ArrayList<>(request.getQueries().size());
    for (SubQuery options : request.getQueries()) {
      Query query = Query.create(options.metric, options.getTags(), getAggregator(options.getAggregator()));
      query.setInterpolator(getInterpolator(options.getAggregator()));
      if (options.getDownsample() != null) {
        setDownsampler(options.getDownsample(), query);
      }
      query.setStart(request.getStart());
      if (request.getEnd() != 0) {
        query.setEnd(request.getEnd());
      }

      queries.add(query);


    }

    LOG.debug("Parsed queries {} from request {}", queries, request);

    List<Result> results = new ArrayList<>(queries.size());
    for (Query query : queries) {
      List<Datapoints> datasets = ts.query(query);
      for (Datapoints dataset : datasets) {
        results.add(new Result(dataset, query.getTags()));
      }
    }
    LOG.info("{} results for request: {}, queries: {}", results, request, queries);
    return results;
  }

  @POST
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  public List<Result> queryFormURLEncoded(String request) throws Exception {
    LOG.info("Form Request: {}", request);
    return query(mapper.treeToValue(mapper.readTree(request), Request.class));
  }

  private static TimeUnit getTimeUnit(String timeUnit) {
    switch (timeUnit) {
      case "nanoseconds":
      case "nanos":
      case "ns":
        return TimeUnit.NANOSECONDS;
      case "microseconds":
      case "micros":
      case "μs":
        return TimeUnit.MICROSECONDS;
      case "milliseconds":
      case "millis":
      case "ms":
        return TimeUnit.MILLISECONDS;
      case "seconds":
      case "s":
        return TimeUnit.SECONDS;
      case "minutes":
      case "m":
        return TimeUnit.MINUTES;
      case "hours":
      case "h":
        return TimeUnit.HOURS;
      case "days":
      case "d":
        return TimeUnit.DAYS;
      default:
        throw new WebApplicationException("Unknown time unit: " + timeUnit, Response.Status.BAD_REQUEST);
    }
  }

  private static Aggregator getAggregator(String aggregation) {
    switch (aggregation) {
      case "avg": return Aggregators.mean();
      case "min": return Aggregators.min();
      case "max": return Aggregators.max();
      case "sum": return Aggregators.sum();
      default: throw new WebApplicationException("No such aggregation function: " + aggregation,
                                                 Response.Status.BAD_REQUEST);
    }
  }

  private static Interpolators.Interpolator getInterpolator(String aggregation) {
    switch (aggregation) {
      case "avg":
      case "min":
      case "max":
      case "sum": return Interpolators.linear();
      default: throw new WebApplicationException("No such aggregation function: " + aggregation,
                                                 Response.Status.BAD_REQUEST);
    }
  }

  private static void setDownsampler(String downsampler, Query query) {
    Matcher matcher = INTERPOLATOR_REGEX.matcher(downsampler);
    if (!matcher.matches()) throw new WebApplicationException("Illegal downsampler: " + downsampler,
                                                              Response.Status.BAD_REQUEST);

    Long value = Longs.tryParse(matcher.group(1));
    if (value == null) throw new WebApplicationException("Unable to parse downsampler interval value: " + downsampler,
                                                         Response.Status.BAD_REQUEST);

    long interval = getTimeUnit(matcher.group(2)).convert(value, TimeUnit.MICROSECONDS);

    Aggregator aggregator = getAggregator(matcher.group(3));
    query.setDownsampler(aggregator, interval);
  }

  static class Request {
    private final long start;
    private final long end;
    private final List<SubQuery> queries;

    @JsonCreator
    public Request(@JsonProperty("start") long start,
                   @JsonProperty("end") long end,
                   @JsonProperty("queries") List<SubQuery> queries,
                   @JsonProperty("msResolution") BooleanFlag msResolution,
                   @JsonProperty("globalAnnotations") BooleanFlag globalAnnotations) {
      if (msResolution.get()) {
        this.start = start * 1000;
        this.end = end * 1000;
      } else {
        this.start = start * 1000 * 1000;
        this.end = end * 1000 * 1000;
      }
      this.queries = queries;
    }

    public long getStart() {
      return start;
    }

    public long getEnd() {
      return end;
    }

    public List<SubQuery> getQueries() {
      return queries;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
                        .add("start", start)
                        .add("end", end)
                        .add("queries", queries)
                        .toString();
    }
  }

  static class SubQuery {
    private String metric;
    private Map<String, String> tags;
    private String aggregator;
    private String downsample;

    @JsonCreator
    public SubQuery(@JsonProperty("metric") String metric,
                    @JsonProperty("tags") Map<String, String> tags,
                    @JsonProperty("aggregator") String aggregator,
                    @JsonProperty("downsample") String downsample) {
      this.metric = metric;
      this.tags = tags;
      this.aggregator = aggregator;
      this.downsample = downsample;
    }

    public String getMetric() {
      return metric;
    }

    public Map<String, String> getTags() {
      return tags;
    }

    public String getAggregator() {
      return aggregator;
    }

    public String getDownsample() {
      return downsample;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
                        .add("metric", metric)
                        .add("tags", tags)
                        .add("aggregator", aggregator)
                        .add("downsample", downsample)
                        .toString();
    }
  }

  private static class Result {
    private Map<String, String> tags;
    private final Datapoints datapoints;

    public Result(Datapoints datapoints, Map<String, String> tags) {
      this.tags = tags;
      this.datapoints = datapoints;
    }

    @JsonProperty("metric")
    public String getMetric() {
      return datapoints.getMetric();
    }

    @JsonProperty("dps")
    public Map<Long, Double> getDatapoints() {
      Map<Long, Double> dps = new TreeMap<>();
      for (Datapoint dp : datapoints) {
        dps.put(dp.getTime() / 1000, dp.getValue());
      }

      LOG.info("dps: {}", dps);
      return dps;
    }

    @JsonProperty("tags")
    Map<String, String> getTags() {
      return tags;
    }

    @JsonProperty("aggregatedTags")
    List<String> getAggregatedTags() {
      return ImmutableList.of();
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
                        .add("datapoints", datapoints)
                        .add("tags", getTags())
                        .add("aggregatedTags", getAggregatedTags())
                        .toString();
    }
  }
}
