package org.kududb.ts.core;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.annotation.concurrent.ThreadSafe;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.client.AbstractionBulldozer;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code Tagsets} manages looking up tagset IDs and tagsets in the
 * {@code tagsets} table. Tagsets and IDs are cached internally, so that
 * subsequent lookups on the same tagset or ID are fast. If a tagset isn't found
 * during a lookup, it is automatically inserted into the {@code tagsets} table,
 * and its tags are inserted into the {@code tags} table.
 *
 * To guarantee that tagset IDs are unique, the {@code tagsets} table is
 * structured as a linear-probe hash table. The tagset is transformed into a
 * canonical byte representation using an internal protobuf format, and the hash
 * of this canonical value is used as the tagset ID. On ID collision, linear
 * probing is used to find a new ID.
 *
 * Internally, {@code Tagsets} keeps an LRU cache of tagsets and IDs so that
 * lookups of frequently used tagsets are fast.
 *
 * Steps for looking up a new tagset:
 *
 *  1) the tagset is converted to a canonical byte string format
 *     (see {@link SerializedTagset}).
 *  2) the internal LRU cache is queried with the byte string, but the lookup fails.
 *  3) a hash of the tagset's byte string is created with the MurmurHash3_32
 *     algorithm (see {@link SerializedTagset#hashCode}).
 *  4) up to {@link #TAGSETS_PER_SCAN} tagsets are scanned from the {@code tagsets}
 *     table beginning with the computed hash as the ID.
 *  5) the tagsets returned in the scan are checked in ID order. If the tagset
 *     is found, the corresponding ID is returned. If there is an ID missing
 *     in the results, then the tagset is inserted with that ID (go to step 6).
 *     If {@link #TAGSETS_PER_SCAN} IDs are present, but the tagset isn't found,
 *     then a new scan is started (step 4), but using the next unscanned ID as
 *     the start ID.
 *  6) the ID from step 5 is used to insert the rowset into the {@code rowsets}
 *     table. If the insert results in a duplicate primary key error, then
 *     another client has concurrently inserted a rowset using the ID. The
 *     concurrently inserted rowset may or may not match the rowset we tried to
 *     insert, so we return to step 4 using the duplicate ID as the start ID.
 *  7) After inserting the tagset successfully in step 6, every tag in the
 *     tagset is inserted into the {@code tags} table. No duplicate errors are
 *     expected in this step.
 *
 * Tagset IDs are 32bits, which allows for hundreds of millions of unique tagsets
 * without risking excessive hash collisions.
 */
@InterfaceAudience.Private
@ThreadSafe
class Tagsets {
  private static final Logger LOG = LoggerFactory.getLogger(Tagsets.class);

  /**
   * Number of tags to return per tagset scanner.
   */
  private static final long TAGSETS_PER_SCAN = 10;

  private final AsyncKuduClient client;
  private final Tags tags;
  private final KuduTable tagsetsTable;
  private final List<Integer> columnIndexes;

  /**
   * Allows tests to hardcode the tagset hash so that collisions can be simulated.
   */
  private Integer hashForTesting = null;

  /**
   * Map of tagset to tagset ID.
   */
  private final LoadingCache<SerializedTagset, Deferred<Integer>> tagsets;

  Tagsets(AsyncKuduClient client, Tags tags, KuduTable tagsetsTable) {
    this.client = client;
    this.tagsetsTable = tagsetsTable;
    this.tags = tags;
    this.columnIndexes = ImmutableList.of(Tables.TAGSETS_ID_INDEX,
                                          Tables.TAGSETS_TAGSET_INDEX);
    this.tagsets = CacheBuilder.newBuilder()
        .maximumSize(1024 * 1024)
        .build(new CacheLoader<SerializedTagset, Deferred<Integer>>() {
          @Override
          public Deferred<Integer> load(SerializedTagset tagset) {
            return lookupOrInsertTagset(tagset, hashForTesting == null ?
                                                tagset.hashCode() : hashForTesting);
          }
        });
  }

  /**
   * Get the ID for a tagset. If the tagset doesn't already have an assigned ID,
   * then a new ID entry will be inserted into the {@code tagset} table, and new
   * tag entries added to the {@code tags} table.
   *
   * @param tagset the tagset
   * @return the ID for the tagset
   */
  Deferred<Integer> getTagsetID(SortedMap<String, String> tagset) {
    return tagsets.getUnchecked(new SerializedTagset(tagset));
  }

  @VisibleForTesting
  void clear() {
    tagsets.invalidateAll();
  }

  /**
   * Sets a constant hash value for all tagsets. Allows simulating hash
   * collisions in relatively small tables.
   *
   * @param hashForTesting the overflow hash value
   */
  @VisibleForTesting
  void setHashForTesting(int hashForTesting) {
    this.hashForTesting = hashForTesting;
  }

  /**
   * Reads the ID of a tagset from the {@code tagset} table. If the tagset
   * doesn't exist in the table, then it is added along with corresponding
   * entries in the {@code tags} table.
   *
   * @param tagset the serialized tagset
   * @return the tagset ID
   */
  private Deferred<Integer> lookupOrInsertTagset(final SerializedTagset tagset, final int id) {
    final class InsertCB implements Callback<Deferred<Integer>, Boolean> {
      private final int probe;

      public InsertCB(int probe) {
        this.probe = probe;
      }

      @Override
      public Deferred<Integer> call(Boolean success) {
        if (success) {
          return tags.insertTagset(probe, tagset.deserialize());
        } else {
          return lookupOrInsertTagset(tagset, probe);
        }
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper(this).add("probe", probe).toString();
      }
    }
    final class LookupCB implements Callback<Deferred<Integer>, TagsetLookupResult> {
      @Override
      public Deferred<Integer> call(TagsetLookupResult result) {
        if (result.found) {
          return Deferred.fromResult(result.id);
        } else {
          int probe = result.id;
          return insertTagset(tagset, probe).addCallbackDeferring(new InsertCB(probe));
        }
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper(this).toString();
      }
    }
    final class LookupErrback implements Callback<Exception, Exception> {
      @Override
      public Exception call(Exception e) throws Exception {
        tagsets.invalidate(tagset);
        return e;
      }
      @Override
      public String toString() {
        return MoreObjects.toStringHelper(this).toString();
      }
    }

    return lookupTagset(tagset, id).addCallbackDeferring(new LookupCB())
                                   .addErrback(new LookupErrback());
  }

  private Deferred<TagsetLookupResult> lookupTagset(SerializedTagset tagset, int id) {
    LOG.debug("Looking up tagset; id: {}, tags: {}", id, tagset);
    AsyncKuduScanner tagsetScanner = tagsetScanner(id);

    return tagsetScanner.nextRows().addCallbackDeferring(
        new TagsetScanCB(tagset, id, tagsetScanner));
  }

  /**
   * Creates an {@link AsyncKuduScanner} over the tagset table beginning with
   * the specified ID.
   *
   * @param id the ID to begin scanning from
   * @return the scanner
   */
  private AsyncKuduScanner tagsetScanner(int id) {
    AsyncKuduScanner.AsyncKuduScannerBuilder scanBuilder = client.newScannerBuilder(tagsetsTable);
    scanBuilder.addPredicate(KuduPredicate.newComparisonPredicate(Tables.TAGSETS_ID_COLUMN,
                                                                  ComparisonOp.GREATER_EQUAL,
                                                                  id));
    if (id < Integer.MAX_VALUE - TAGSETS_PER_SCAN) {
      scanBuilder.addPredicate(KuduPredicate.newComparisonPredicate(Tables.TAGSETS_ID_COLUMN,
                                                                    ComparisonOp.LESS,
                                                                    id + TAGSETS_PER_SCAN));
    }
    scanBuilder.setProjectedColumnIndexes(columnIndexes);
    AbstractionBulldozer.sortResultsByPrimaryKey(scanBuilder);
    return scanBuilder.build();
  }

  /**
   * Attempts to insert the provided tagset and ID. Returns {@code true} if the
   * write was successful, or {@code false} if the write failed due to a tagset
   * with the same ID already existing in the table.
   *
   * @param tagset the tagset to insert
   * @param id     the ID to insert the tagset with
   * @return whether the write succeeded
   */
  private Deferred<Boolean> insertTagset(final SerializedTagset tagset, final int id) {
    final class InsertTagsetCB implements Callback<Deferred<Boolean>, OperationResponse> {
      @Override
      public Deferred<Boolean> call(OperationResponse response) {
        if (response.hasRowError()) {
          if (response.getRowError().getErrorStatus().isAlreadyPresent()) {
            LOG.info("Attempted to insert duplicate tagset; id: {}, tagset: {}", id, tagset);
            // TODO: Consider adding a backoff with jitter before attempting
            //       the insert again (if the lookup fails).
            return Deferred.fromResult(false);
          }
          return Deferred.fromError(new RuntimeException(
              String.format("Unable to insert tagset; id: %s, tagset: %s, error: %s",
                            id, tagset, response.getRowError())));
        } else {
          return Deferred.fromResult(true);
        }
      }
      @Override
      public String toString() {
        return MoreObjects.toStringHelper(this).toString();
      }
    }

    LOG.debug("Inserting tagset; id: {}, tags: {}", id, tagset);
    final AsyncKuduSession session = client.newSession();
    try {
      // We don't have to handle PleaseThrottleException because we are only
      // inserting a single row.
      final Insert insert = tagsetsTable.newInsert();
      insert.getRow().addInt(Tables.TAGSETS_ID_INDEX, id);
      insert.getRow().addBinary(Tables.TAGSETS_TAGSET_INDEX, tagset.getBytes());
      return session.apply(insert).addCallbackDeferring(new InsertTagsetCB());
    } finally {
      session.close();
    }
  }

  /**
   * The result of a tagset lookup. If {@link #found} is {@code true},
   * then the tagset was found in the table, and {@link #id} will contain the
   * tagset's ID. If {@link #found} is {@link false}, then the tagset was
   * not found in the table, and {@link #id} will contain the ID that the
   * tagset should be inserted with.
   */
  private static class TagsetLookupResult {
    private final int id;
    private final boolean found;

    private TagsetLookupResult(boolean found, int id) {
      this.found = found;
      this.id = id;
    }
  }

  /**
   * Finds a tagset in the {@code tagset} table.
   */
  private final class TagsetScanCB implements Callback<Deferred<TagsetLookupResult>, RowResultIterator> {
    private final SerializedTagset tagset;
    private AsyncKuduScanner scanner;
    private int id;
    private int probe;

    /**
     * Create a new {@code TagsetScanCB} looking for a tagset starting with the provided ID.
     *
     * @param tagset  the tagset being looked up
     * @param id      the ID that the scanner is looking up
     * @param scanner the initialscanner
     */
    TagsetScanCB(SerializedTagset tagset, int id, AsyncKuduScanner scanner) {
      this.tagset = tagset;
      this.scanner = scanner;
      this.id = id;
      this.probe = id;
    }

    @Override
    public Deferred<TagsetLookupResult> call(RowResultIterator rows) {
      for (RowResult row : rows) {
        int id = row.getInt(Tables.TAGSETS_ID_INDEX);
        Preconditions.checkState(id >= probe);
        if (id != probe) {
          // We found a hole in the table where we expected the tagset.
          return Deferred.fromResult(new TagsetLookupResult(false, probe));
        }

        if (tagset.equals(row.getBinary(Tables.TAGSETS_TAGSET_INDEX))) {
          return Deferred.fromResult(new TagsetLookupResult(true, id));
        }

        probe++;
      }

      // We probed through the entire RowResult and didn't find the tagset.
      if (!scanner.hasMoreRows()) {
        if (probe <= Ints.saturatedCast((long) id + TAGSETS_PER_SCAN)) {
          // We found a hole at the end of the scan.
          return Deferred.fromResult(new TagsetLookupResult(false, probe));
        }
        // The current scanner has been exhausted; create a new scanner from the
        // latest probe point.
        scanner = tagsetScanner(probe);
        id = probe;
      }
      return scanner.nextRows().addCallbackDeferring(this);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
                        .add("id", id)
                        .add("tags", tagset)
                        .toString();
    }
  }

  /**
   * Serializes a set of tags into a canonical byte format for storing in the
   * {@code tagsets} table. The format consists of the sorted sequence of key
   * and value strings serialized with a leading two byte length header, then
   * the UTF-8 encoded bytes.
   *
   * The hash of the tagset is computed over the serialized bytes using the
   * Murmur3_32 algorithm.
   */
  @VisibleForTesting
  static class SerializedTagset {
    private final byte[] bytes;

    public SerializedTagset(SortedMap<String, String> tagset) {
      try {
        int lengthEstimate = 0;
        for (Map.Entry<String, String> tags : tagset.entrySet()) {
          lengthEstimate += tags.getKey().length() + tags.getValue().length() + 4;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream(lengthEstimate);
        DataOutputStream dos = new DataOutputStream(baos);

        for (Map.Entry<String, String> tag : tagset.entrySet()) {
          dos.writeUTF(tag.getKey());
          dos.writeUTF(tag.getValue());
        }
        dos.flush();
        baos.flush();
        bytes = baos.toByteArray();
      } catch (IOException e) {
        throw new RuntimeException("unreachable");
      }
    }

    /**
     * Returns the serialized tagset. The caller must not modify the array.
     * @return the serialized tagset
     */
    public byte[] getBytes() {
      return bytes;
    }

    public SortedMap<String, String> deserialize() {
      try {
        SortedMap<String, String> tags = new TreeMap<>();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
        while (dis.available() > 0) {
          tags.put(dis.readUTF(), dis.readUTF());
        }
        return tags;
      } catch (IOException e) {
        throw new RuntimeException("unreachable");
      }
    }

    /**
     * Compare against a byte buffer. Useful for comparing a
     * {@code SerializedTagset} against a Kudu column.
     * @param buf the byte buffer
     * @return whether the byte buffer contains this serialized tagset
     */
    public boolean equals(ByteBuffer buf) {
      if (buf.limit() - buf.position() != bytes.length) return false;

      for (int i = 0, j = buf.position(); i < bytes.length; i++, j++) {
        if (bytes[i] != buf.get(j)) return false;
      }
      return true;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null) return false;
      else if (o.getClass() != getClass()) return false;
      SerializedTagset that = (SerializedTagset) o;
      return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
      return Hashing.murmur3_32().hashBytes(bytes).asInt();
    }

    @Override
    public String toString() {
      return deserialize().toString();
    }
  }
}
