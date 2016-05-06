package org.kududb.ts.core;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.stumbleupon.async.Deferred;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;

import org.junit.Test;
import org.kududb.client.BaseKuduTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTagsets extends BaseKuduTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestTagsets.class);

  @Test(timeout = 10000)
  public void testTagsetLookup() throws Exception {
    try (KuduTS ts = KuduTS.openOrCreate(ImmutableList.of(masterAddresses),
                                         "testTagsetLookup",
                                         CreateOptions.defaults())) {
      Tagsets tagsets = ts.getTagsets();
      SortedMap<String, String> tagset = ImmutableSortedMap.of("k1", "v1");

      int tagsetHash = new Tagsets.SerializedTagset(tagset).hashCode();

      int insertID = tagsets.getTagsetID(tagset).join();
      tagsets.clear();
      int lookupID = tagsets.getTagsetID(tagset).join();

      assertEquals(tagsetHash, insertID);
      assertEquals(insertID, lookupID);
    }
  }

  @Test(timeout = 10000)
  public void testConcurrentLookup() throws Exception {
    try (KuduTS ts = KuduTS.openOrCreate(ImmutableList.of(masterAddresses),
                                         "testConcurrentLookup",
                                         CreateOptions.defaults())) {
      Tagsets tagsets = ts.getTagsets();
      SortedMap<String, String> tagset = ImmutableSortedMap.of("k1", "v1");

      List<Deferred<Integer>> deferreds = new ArrayList<>();

      for (int i = 0; i < 10; i++) {
        deferreds.add(tagsets.getTagsetID(tagset));
        tagsets.clear();
      }

      assertEquals(1, ImmutableSet.copyOf(Deferred.group(deferreds).join()).size());
    }
  }

  @Test(timeout = 10000)
  public void testEmptyTagsetLookup() throws Exception {
    try (KuduTS ts = KuduTS.openOrCreate(ImmutableList.of(masterAddresses),
                                         "testEmptyTagsetLookup",
                                         CreateOptions.defaults())) {
      Tagsets tagsets = ts.getTagsets();
      int id = tagsets.getTagsetID(ImmutableSortedMap.<String, String>of()).join();
      assertEquals(0, id);
    }
  }

  @Test(timeout = 10000)
  public void testHashWraparound() throws Exception {
    try (KuduTS ts = KuduTS.openOrCreate(ImmutableList.of(masterAddresses),
                                         "testHashWraparound",
                                         CreateOptions.defaults())) {
      Tagsets tagsets = ts.getTagsets();

      tagsets.setHashForTesting(Integer.MAX_VALUE - 9);

      int id = tagsets.getTagsetID(ImmutableSortedMap.of("key", "val")).join();
      assertEquals(Integer.MAX_VALUE - 9, id);
    }
  }

  @Test(timeout = 10000)
  public void testOverlappingTagsets() throws Exception {
    try (KuduTS ts = KuduTS.openOrCreate(ImmutableList.of(masterAddresses),
                                         "testOverlappingTagsets",
                                         CreateOptions.defaults())) {
      Tagsets tagsets = ts.getTagsets();
      int id1 = tagsets.getTagsetID(ImmutableSortedMap.of("k1", "v1")).join();
      int id2 = tagsets.getTagsetID(ImmutableSortedMap.of("k2", "v2")).join();
      int id3 = tagsets.getTagsetID(ImmutableSortedMap.of("k1", "v1", "k2", "v2", "k3", "v3")).join();
      int id4 = tagsets.getTagsetID(ImmutableSortedMap.of("k1", "v2")).join();
      int id5 = tagsets.getTagsetID(ImmutableSortedMap.of("k2", "v1")).join();
      int id6 = tagsets.getTagsetID(ImmutableSortedMap.<String, String>of()).join();
      assertEquals(6, ImmutableSet.of(id1, id2, id3, id4, id5, id6).size());
    }
  }

  @Test(timeout = 100000)
  public void testHashCollisions() throws Exception {
    try (KuduTS ts = KuduTS.openOrCreate(ImmutableList.of(masterAddresses),
                                         "testHashCollisions",
                                         CreateOptions.defaults())) {
      Tagsets tagsets = ts.getTagsets();

      int numTagsets = 100;
      tagsets.setHashForTesting(0);

      List<Deferred<Integer>> deferreds = new ArrayList<>();
      for (int i = 0; i <= numTagsets; i++) {
        deferreds.add(tagsets.getTagsetID(ImmutableSortedMap.of("key", Integer.toString(i))));
      }

      List<Integer> ids = Deferred.group(deferreds).join();
      Collections.sort(ids);

      for (int i = 0; i < numTagsets; i++) {
        assertEquals(i, ids.get(i).intValue());
      }
    }
  }

  @Test(timeout = 10000)
  public void testHashCollisionsWraparound() throws Exception {
    try (KuduTS ts = KuduTS.openOrCreate(ImmutableList.of(masterAddresses),
                                         "testHashCollisionsWraparound",
                                         CreateOptions.defaults()
                                                      .setNumTagsetsTablets(2))) {
      Tagsets tagsets = ts.getTagsets();

      int numTagsets = 30;
      int offset = 15;

      tagsets.setHashForTesting(Integer.MAX_VALUE - offset);

      List<Deferred<Integer>> deferreds = new ArrayList<>();
      for (int i = 0; i < numTagsets; i++) {
        deferreds.add(tagsets.getTagsetID(ImmutableSortedMap.of("key", Integer.toString(i))));
      }

      List<Integer> ids = Deferred.group(deferreds).join();
      Collections.sort(ids);

      List<Integer> expectedIds = new ArrayList<>();
      for (int i = 0; i < numTagsets; i++) {
        expectedIds.add((Integer.MAX_VALUE - offset) + i);
      }
      Collections.sort(expectedIds);
      assertEquals(expectedIds, ids);
    }
  }
}