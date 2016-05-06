package org.kududb.ts.argus;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.salesforce.dva.argus.entity.Annotation;
import com.salesforce.dva.argus.entity.Metric;
import com.salesforce.dva.argus.entity.TSDBEntity;
import com.salesforce.dva.argus.service.DefaultService;
import com.salesforce.dva.argus.service.MonitorService;
import com.salesforce.dva.argus.service.TSDBService;
import com.salesforce.dva.argus.service.tsdb.AnnotationQuery;
import com.salesforce.dva.argus.service.tsdb.MetricQuery;
import com.salesforce.dva.argus.system.SystemConfiguration;
import com.salesforce.dva.argus.system.SystemException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.kududb.client.OperationResponse;
import org.kududb.client.SessionConfiguration;
import org.kududb.ts.core.Aggregator;
import org.kududb.ts.core.Aggregators;
import org.kududb.ts.core.CreateOptions;
import org.kududb.ts.core.Datapoint;
import org.kududb.ts.core.Datapoints;
import org.kududb.ts.core.Interpolators;
import org.kududb.ts.core.KuduTS;
import org.kududb.ts.core.Query;
import org.kududb.ts.core.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuduService extends DefaultService implements TSDBService {
  protected Logger _logger = LoggerFactory.getLogger(getClass());

  private final KuduTS ts;

  @Inject
  public KuduService(SystemConfiguration config, MonitorService monitorService) throws Exception {
    super(config);
    ts = KuduTS.openOrCreate(Lists.newArrayList("locahost"), "argus", CreateOptions.defaults());
  }

  @Override
  public void putMetrics(List<Metric> metrics) {
    _logger.debug("writing metrics {}", metrics);

    WriteBatch batch = ts.writeBatch();
    batch.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    try {
      try {
        for (Metric metric : metrics) {
          SortedMap<String, String> tags = new TreeMap<>(metric.getTags());
          String metricName = metric.getScope() + "." + metric.getMetric();
          if (metric.getDisplayName() != null && !metric.getDisplayName().isEmpty()) {
            tags.put(TSDBEntity.ReservedField.DISPLAY_NAME.name(), metric.getDisplayName());
          }
          if (metric.getUnits() != null && !metric.getUnits().isEmpty()) {
            tags.put(TSDBEntity.ReservedField.UNITS.name(), metric.getUnits());
          }
          for (Map.Entry<Long, String> dp : metric.getDatapoints().entrySet()) {
            try {
              batch.writeDatapoint(metricName,
                                   tags,
                                   dp.getKey(),
                                   Double.parseDouble(dp.getValue()));
            } catch (Exception e) {
              throw new SystemException(e);
            }
          }
        }

        List<OperationResponse> responses = batch.flush();
        for (OperationResponse response : responses) {
          if (response.hasRowError()) {
            String error = String.format("row error in write batch: {}", response.getRowError());
            _logger.warn("row error in write batch: {}", response.getRowError());
            throw new SystemException(error);
          }
        }
      } finally {
        batch.close();
      }
    } catch (Exception e) {
      throw new SystemException(e);
    }
  }

  @Override
  public Map<MetricQuery, List<Metric>> getMetrics(List<MetricQuery> queries) {
    Map<MetricQuery, List<Metric>> results = new HashMap<>(queries.size());

    for(MetricQuery query : queries) {
      try {
        String metric = query.getScope() + "." + query.getMetric();
        SortedMap<String, String> tags = ImmutableSortedMap.copyOf(query.getTags());

        Aggregator aggregator;
        Interpolators.Interpolator interpolator;
        switch (query.getAggregator()) {
          case MIN: {
            aggregator = Aggregators.min();
            interpolator = Interpolators.linear();
            break;
          }
          case MAX: {
            aggregator = Aggregators.max();
            interpolator = Interpolators.linear();
            break;
          }
          case SUM: {
            aggregator = Aggregators.sum();
            interpolator = Interpolators.linear();
            break;
          }
          case AVG: {
            aggregator = Aggregators.mean();
            interpolator = Interpolators.linear();
            break;
          }
          case DEV: {
            aggregator = Aggregators.standardDeviation();
            interpolator = Interpolators.linear();
            break;
          }
          default:
            throw new SystemException(String.format("Aggregator not supported: %s",
                                                    query.getAggregator()));
        }

        Aggregator downsampler;
        switch (query.getDownsampler()) {
          case MIN: {
            downsampler = Aggregators.min();
            break;
          }
          case MAX: {
            downsampler = Aggregators.max();
            break;
          }
          case SUM: {
            downsampler = Aggregators.sum();
            break;
          }
          case AVG: {
            downsampler = Aggregators.mean();
            break;
          }
          case DEV: {
            downsampler = Aggregators.standardDeviation();
            break;
          }
          default:
            throw new SystemException(String.format("Downsampler not supported: %s",
                                                    query.getAggregator()));
        }

        Query kuduQuery = Query.create(metric, tags, aggregator)
                               .setStart(query.getStartTimestamp() * 1000)
                               .setEnd(query.getEndTimestamp() * 1000)
                               .setDownsampler(downsampler, query.getDownsamplingPeriod() * 1000)
                               .setInterpolator(interpolator);

        List<Datapoints> groups = ts.query(kuduQuery);

        List<Metric> resultMetrics = new ArrayList<>(groups.size());
        for (Datapoints datapoints : groups) {
          Metric result = new Metric(query.getScope(), query.getMetric());
          result.setTags(tags);
          result.setDisplayName(metric);
          HashMap<Long, String> points = new HashMap<>(datapoints.size());
          for (Datapoint datapoint : datapoints) {
            points.put(datapoint.getTime() / 1000, Double.toString(datapoint.getValue()));
          }
          result.setDatapoints(points);
          resultMetrics.add(result);
        }

        _logger.debug("Argus query: {}, Kudu query: {}, Kudu results: {}, Argus results: {}",
                      query, kuduQuery, groups, resultMetrics);
        results.put(query, resultMetrics);
      } catch (Exception e) {
        throw new SystemException(e);
      }
    }
    return results;
  }

  @Override
  public void putAnnotations(List<Annotation> annotations) {
    // TODO Auto-generated method stub

  }

  @Override
  public List<Annotation> getAnnotations(List<AnnotationQuery> queries) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String constructTSDBMetricName(String scope, String namespace) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getScopeFromTSDBMetric(String tsdbMetricName) {
    // TODO
    return null;
  }

  @Override
  public String getNamespaceFromTSDBMetric(String tsdbMetricName) {
    // TODO
    return null;
  }
}
