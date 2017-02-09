package org.kududb.ts.core;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;
import org.apache.kudu.client.BaseKuduTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTags extends BaseKuduTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestTags.class);

  /** Builds a tagset from the provided tags. */
  private static SortedMap<String, String> tagset(String... tags) {
    if (tags.length % 2 == 1) throw new IllegalArgumentException("tags must have key and value");
    SortedMap<String, String> tagset = new TreeMap<>();
    for (int i = 0; i < tags.length; i += 2) {
      tagset.put(tags[i], tags[i + 1]);
    }
    return ImmutableSortedMap.copyOf(tagset);
  }

  @Test
  public void testGetTagsetIDsForTag() throws Exception {
    try (KuduTS ts = KuduTS.openOrCreate(ImmutableList.of(getMasterAddresses()),
                                         "testGetTagsetIDsForTag",
                                         CreateOptions.defaults())) {
      Tags tags = ts.getTags();

      assertEquals(Integer.valueOf(0), tags.insertTagset(0, tagset("k1", "v1")).join());
      assertEquals(Integer.valueOf(1), tags.insertTagset(1, tagset("k1", "v2")).join());
      assertEquals(Integer.valueOf(2), tags.insertTagset(2, tagset("k2", "v1")).join());
      assertEquals(Integer.valueOf(3), tags.insertTagset(3, tagset("k2", "v2")).join());
      assertEquals(Integer.valueOf(4), tags.insertTagset(4, tagset("k1", "v1", "k2", "v1")).join());
      assertEquals(Integer.valueOf(5), tags.insertTagset(5, tagset("k1", "v1", "k2", "v2")).join());
      assertEquals(Integer.valueOf(6), tags.insertTagset(6, tagset("k1", "v2", "k2", "v1")).join());
      assertEquals(Integer.valueOf(7), tags.insertTagset(7, tagset("k1", "v2", "k2", "v2")).join());

      assertEquals(ImmutableList.of(0, 4, 5), tags.getTagsetIDsForTag("k1", "v1").join().asList());
      assertEquals(ImmutableList.of(3, 5, 7), tags.getTagsetIDsForTag("k2", "v2").join().asList());
      assertEquals(ImmutableList.of(), tags.getTagsetIDsForTag("k1", "v3").join().asList());
    }
  }

  @Test
  public void testGetTagsetIDsForTags() throws Exception {
    try (KuduTS ts = KuduTS.openOrCreate(ImmutableList.of(getMasterAddresses()),
                                         "testGetTagsetIDsForTags",
                                         CreateOptions.defaults())) {
      Tags tags = ts.getTags();

      assertEquals(Integer.valueOf(0), tags.insertTagset(0, tagset("k1", "v1")).join());
      assertEquals(Integer.valueOf(1), tags.insertTagset(1, tagset("k1", "v2")).join());
      assertEquals(Integer.valueOf(2), tags.insertTagset(2, tagset("k2", "v1")).join());
      assertEquals(Integer.valueOf(3), tags.insertTagset(3, tagset("k2", "v2")).join());
      assertEquals(Integer.valueOf(4), tags.insertTagset(4, tagset("k1", "v1", "k2", "v1")).join());
      assertEquals(Integer.valueOf(5), tags.insertTagset(5, tagset("k1", "v1", "k2", "v2")).join());
      assertEquals(Integer.valueOf(6), tags.insertTagset(6, tagset("k1", "v2", "k2", "v1")).join());
      assertEquals(Integer.valueOf(7), tags.insertTagset(7, tagset("k1", "v2", "k2", "v2")).join());

      assertEquals(ImmutableList.of(4), tags.getTagsetIDsForTags(ImmutableMap.of("k1", "v1", "k2", "v1")).join().asList());
      assertEquals(ImmutableList.of(), tags.getTagsetIDsForTags(ImmutableMap.of("k1", "v1", "k2", "v3")).join().asList());
      assertEquals(ImmutableList.of(5), tags.getTagsetIDsForTags(ImmutableMap.of("k1", "v1", "k2", "v2")).join().asList());
    }
  }
}