/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.service.cql;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CQLDistributedMapCacheQueryTest {

    @Test
    void testBuildPutCqlWithoutTtl() {
        assertEquals("INSERT INTO cache_table (cache_key,cache_value) VALUES (?,?)",
                CQLDistributedMapCache.buildPutCql("cache_table", "cache_key", "cache_value", null, false));
    }

    @Test
    void testBuildPutCqlWithTtl() {
        assertEquals("INSERT INTO cache_table (cache_key,cache_value) VALUES (?,?) USING TTL 3600",
                CQLDistributedMapCache.buildPutCql("cache_table", "cache_key", "cache_value", 3600, false));
    }

    @Test
    void testBuildPutCqlBucketedIncludesBucketColumn() {
        assertEquals("INSERT INTO cache_table (bucket,cache_key,cache_value) VALUES (?,?,?)",
                CQLDistributedMapCache.buildPutCql("cache_table", "cache_key", "cache_value", null, true));
    }

    @Test
    void testBuildPutIfAbsentCqlUsesLightweightTransaction() {
        final String cql = CQLDistributedMapCache.buildPutIfAbsentCql("cache_table", "cache_key", "cache_value", null, false);
        assertEquals("INSERT INTO cache_table (cache_key,cache_value) VALUES (?,?) IF NOT EXISTS", cql);
    }

    @Test
    void testBuildPutIfAbsentCqlWithTtlIncludesBothClauses() {
        final String cql = CQLDistributedMapCache.buildPutIfAbsentCql("cache_table", "cache_key", "cache_value", 60, false);
        assertEquals("INSERT INTO cache_table (cache_key,cache_value) VALUES (?,?) IF NOT EXISTS USING TTL 60", cql);
    }

    @Test
    void testBuildPutIfAbsentCqlBucketedIncludesBucketColumn() {
        final String cql = CQLDistributedMapCache.buildPutIfAbsentCql("cache_table", "cache_key", "cache_value", null, true);
        assertEquals("INSERT INTO cache_table (bucket,cache_key,cache_value) VALUES (?,?,?) IF NOT EXISTS", cql);
    }

    @Test
    void testBuildFetchCqlSelectsOnlyValueColumn() {
        final String cql = CQLDistributedMapCache.buildFetchCql("cache_table", "cache_key", "cache_value", false);
        assertEquals("SELECT cache_value FROM cache_table WHERE cache_key=?", cql);
    }

    @Test
    void testBuildFetchCqlBucketedMatchesOnBucketAndKey() {
        final String cql = CQLDistributedMapCache.buildFetchCql("cache_table", "cache_key", "cache_value", true);
        assertEquals("SELECT cache_value FROM cache_table WHERE bucket=? AND cache_key=?", cql);
    }

    @Test
    void testBuildExistsCqlSelectsOnlyKeyColumnWithLimitOne() {
        final String cql = CQLDistributedMapCache.buildExistsCql("cache_table", "cache_key", false);
        assertEquals("SELECT cache_key FROM cache_table WHERE cache_key=? LIMIT 1", cql);
        assertTrue(cql.endsWith("LIMIT 1"), "containsKey must not fetch the value column or aggregate with COUNT(*)");
    }

    @Test
    void testBuildExistsCqlBucketedMatchesOnBucketAndKey() {
        final String cql = CQLDistributedMapCache.buildExistsCql("cache_table", "cache_key", true);
        assertEquals("SELECT cache_key FROM cache_table WHERE bucket=? AND cache_key=? LIMIT 1", cql);
    }

    @Test
    void testBuildDeleteCqlIsUnconditional() {
        final String cql = CQLDistributedMapCache.buildDeleteCql("cache_table", "cache_key", false);
        assertEquals("DELETE FROM cache_table WHERE cache_key=?", cql);
    }

    @Test
    void testBuildDeleteCqlBucketedMatchesOnBucketAndKey() {
        final String cql = CQLDistributedMapCache.buildDeleteCql("cache_table", "cache_key", true);
        assertEquals("DELETE FROM cache_table WHERE bucket=? AND cache_key=?", cql);
    }

    @Test
    void testBuildStrictDeleteCqlUsesIfExists() {
        final String cql = CQLDistributedMapCache.buildStrictDeleteCql("cache_table", "cache_key", false);
        assertEquals("DELETE FROM cache_table WHERE cache_key=? IF EXISTS", cql);
    }

    @Test
    void testBuildStrictDeleteCqlBucketedMatchesOnBucketAndKey() {
        final String cql = CQLDistributedMapCache.buildStrictDeleteCql("cache_table", "cache_key", true);
        assertEquals("DELETE FROM cache_table WHERE bucket=? AND cache_key=? IF EXISTS", cql);
    }

    @Test
    void testComputeBucketIsWithinRange() {
        for (int i = 0; i < 1000; i++) {
            final byte[] keyBytes = ("key-" + i).getBytes(StandardCharsets.UTF_8);
            final int bucket = CQLDistributedMapCache.computeBucket(keyBytes, 32_768);
            assertTrue(bucket >= 0 && bucket < 32_768, "bucket " + bucket + " out of range for key-" + i);
        }
    }

    @Test
    void testComputeBucketIsStableForTheSameKey() {
        final byte[] keyBytes = "stable-key".getBytes(StandardCharsets.UTF_8);
        final int first = CQLDistributedMapCache.computeBucket(keyBytes, 1_000_000);
        final int second = CQLDistributedMapCache.computeBucket(keyBytes, 1_000_000);
        assertEquals(first, second);
    }

    @Test
    void testComputeBucketDistributesDifferentKeysReasonablyUniformly() {
        // Not a rigorous statistical test -- just a sanity check that CRC32-based bucketing doesn't collapse a
        // moderate number of distinct keys into a handful of buckets, which would defeat the point of bucketing.
        final int partitionCount = 1000;
        final Set<Integer> observedBuckets = new HashSet<>();
        for (int i = 0; i < 10_000; i++) {
            observedBuckets.add(CQLDistributedMapCache.computeBucket(("key-" + i).getBytes(StandardCharsets.UTF_8), partitionCount));
        }
        assertTrue(observedBuckets.size() > partitionCount / 2,
                "expected most of the " + partitionCount + " buckets to be hit at least once across 10,000 distinct keys, only saw " + observedBuckets.size());
    }

    @Test
    void testParseContactPointsUsesDefaultPortWhenOmitted() {
        final List<InetSocketAddress> contactPoints = CQLDistributedMapCache.parseContactPoints("node1");
        assertEquals(1, contactPoints.size());
        assertEquals("node1", contactPoints.get(0).getHostString());
        assertEquals(CQLDistributedMapCache.DEFAULT_CQL_PORT, contactPoints.get(0).getPort());
    }

    @Test
    void testParseContactPointsHandlesMultipleHostsAndExplicitPorts() {
        final List<InetSocketAddress> contactPoints = CQLDistributedMapCache.parseContactPoints("node1:9042, node2:9142");
        assertEquals(2, contactPoints.size());
        assertEquals("node1", contactPoints.get(0).getHostString());
        assertEquals(9042, contactPoints.get(0).getPort());
        assertEquals("node2", contactPoints.get(1).getHostString());
        assertEquals(9142, contactPoints.get(1).getPort());
    }

    @Test
    void testParseContactPointsReturnsEmptyListForNull() {
        assertTrue(CQLDistributedMapCache.parseContactPoints(null).isEmpty());
    }
}
