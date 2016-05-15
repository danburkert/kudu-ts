package org.kududb.ts.core;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import javax.annotation.concurrent.ThreadSafe;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.client.AsyncKuduClient;
import org.kududb.client.AsyncKuduScanner;
import org.kududb.client.AsyncKuduSession;
import org.kududb.client.Insert;
import org.kududb.client.KuduPredicate;
import org.kududb.client.KuduPredicate.ComparisonOp;
import org.kududb.client.KuduTable;
import org.kududb.client.OperationResponse;
import org.kududb.client.RowResult;
import org.kududb.client.RowResultIterator;
import org.kududb.client.SessionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code Tagsets} manages inserting and retrieving tags and their associated
 * tagset IDs from the {@code tags} table.
 */
@InterfaceAudience.Private
@ThreadSafe
public class Tags {
  private static final Logger LOG = LoggerFactory.getLogger(Tags.class);

  private static final List<Integer> TAGSET_ID_PROJECTION =
      ImmutableList.of(Tables.TAGS_TAGSET_ID_INDEX);

  private final AsyncKuduClient client;
  private final KuduTable table;

  public Tags(AsyncKuduClient client, KuduTable table) {
    this.client = client;
    this.table = table;
  }

  /**
   * Insert a tagset into the {@code tags} table.
   * @param id the tagset ID.
   * @param tagset the tagset.
   * @return The tagset ID.
   */
  public Deferred<Integer> insertTagset(final int id, final SortedMap<String, String> tagset) {
    if (tagset.isEmpty()) { return Deferred.fromResult(id); }
    LOG.debug("Inserting tags; tagsetID: {}, tags: {}", id, tagset);
    final AsyncKuduSession session = client.newSession();

    class InsertTagsetCB implements Callback<Deferred<Integer>, List<OperationResponse>> {
      @Override
      public Deferred<Integer> call(List<OperationResponse> responses) {
        try {
          for (OperationResponse response : responses) {
            if (response.hasRowError()) {
              return Deferred.fromError(new RuntimeException(
                  String.format("Unable to insert tag: %s", response.getRowError())));
            }
          }
          return Deferred.fromResult(id);
        } finally {
          session.close();
        }
      }
      @Override
      public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("id", id)
                          .add("tags", tagset)
                          .toString();
      }
    }

    if (tagset.size() > 1000) {
      session.setMutationBufferSpace(tagset.size());
    }
    session.setMutationBufferLowWatermark(1.0f);

    // buffer all of the tags into the session, and ensure that we don't get
    // a PleaseThrottleException. In practice the number of tags should be
    // small.
    session.setMutationBufferSpace(tagset.size());
    session.setMutationBufferLowWatermark(1.0f);
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    for (Map.Entry<String, String> tag : tagset.entrySet()) {
      Insert insert = table.newInsert();
      // TODO: check with JD that if the inserts below fail, the error will
      // also be returned in the flush call.
      insert.getRow().addString(Tables.TAGS_KEY_INDEX, tag.getKey());
      insert.getRow().addString(Tables.TAGS_VALUE_INDEX, tag.getValue());
      insert.getRow().addInt(Tables.TAGS_TAGSET_ID_INDEX, id);
      session.apply(insert);
    }
    return session.flush().addCallbackDeferring(new InsertTagsetCB());
  }

  /**
   * Retrieves the tagset IDs of all tagsets which contain the specified tag.
   * The tagset IDs are returned in sorted order.
   *
   * @param key the tag key
   * @param value the tag value
   * @return the sorted tagset IDs
   */
  public Deferred<IntVec> getTagsetIDsForTag(final String key, final String value) {
    AsyncKuduScanner.AsyncKuduScannerBuilder scan = client.newScannerBuilder(table);
    scan.addPredicate(KuduPredicate.newComparisonPredicate(Tables.TAGS_KEY_COLUMN,
                                                           ComparisonOp.EQUAL, key));
    scan.addPredicate(KuduPredicate.newComparisonPredicate(Tables.TAGS_VALUE_COLUMN,
                                                           ComparisonOp.EQUAL, value));
    scan.setProjectedColumnIndexes(TAGSET_ID_PROJECTION);
    final AsyncKuduScanner scanner = scan.build();

    class GetTagCB implements Callback<Deferred<IntVec>, RowResultIterator> {
      private final IntVec tagsetIDs = IntVec.create();
      @Override
      public Deferred<IntVec> call(RowResultIterator results) {
        for (RowResult result : results) {
          tagsetIDs.push(result.getInt(0));
        }
        if (scanner.hasMoreRows()) {
          return scanner.nextRows().addCallbackDeferring(this);
        }
        // The Kudu java client doesn't yet allow us to specify a sorted
        // (fault-tolerant) scan, so have to sort manually.
        tagsetIDs.sort();
        return Deferred.fromResult(tagsetIDs);
      }
      @Override
      public String toString() {
        return MoreObjects.toStringHelper(this).add("key", key).add("value", value).toString();
      }
    }

    return scanner.nextRows().addCallbackDeferring(new GetTagCB());
  }

  private static final class IntersectTagsetsCB implements Callback<IntVec, ArrayList<IntVec>> {
    @Override
    public IntVec call(ArrayList<IntVec> idSets) {
      IntVec intersection = idSets.remove(idSets.size() - 1);
      for (IntVec ids : idSets) {
        intersection.intersect(ids);
      }
      intersection.dedup();
      return intersection;
    }
    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).toString();
    }
  }

  /**
   * Retrieves the tagset IDs of all tagsets which contain all of the specified tags.
   * The tagset IDs are returned in sorted order.
   *
   * @param tags the tags to filter by
   * @return the sorted tagset IDs
   */
  public Deferred<IntVec> getTagsetIDsForTags(Map<String, String> tags) {

    List<Deferred<IntVec>> deferreds = new ArrayList<>(tags.size());
    for (Map.Entry<String, String> tag : tags.entrySet()) {
      deferreds.add(getTagsetIDsForTag(tag.getKey(), tag.getValue()));
    }
    return Deferred.group(deferreds).addCallback(new IntersectTagsetsCB());
  }
}
