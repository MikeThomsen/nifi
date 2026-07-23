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

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.service.cql.api.CQLExecutionService;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Shared CRUD/LWT/TTL coverage for {@link CQLDistributedMapCache}, run against a real cluster. Concrete
 * subclasses are responsible only for starting their own backend's testcontainer, setting {@link #contactPoint}
 * to its {@code host:port}, and calling {@link #bootstrapSchema(String)} once before any {@code @Test} method
 * runs -- everything else (schema shape, cache configuration, and every test method) is shared here so the
 * Cassandra and ScyllaDB integration tests exercise identical behavior against the same DataStax driver
 * dialect. Each subclass is gated to its own Maven profile via {@code @EnabledIfSystemProperty}, so only one
 * backend's container ever starts for a given build.
 */
abstract class AbstractCQLDistributedMapCacheIT {

    static final String KEYSPACE = "testspace";
    static final String LOCAL_DATACENTER = "datacenter1";
    static final String TABLE = "cql_cache";
    static final String BUCKETED_TABLE = "cql_cache_bucketed";
    static final String KEY_COLUMN = "cache_key";
    static final String VALUE_COLUMN = "cache_value";
    static final int TEST_PARTITION_COUNT = 32_768;

    private static final Serializer<String> STRING_SERIALIZER = (value, output) -> output.write(value.getBytes(StandardCharsets.UTF_8));
    private static final Deserializer<String> STRING_DESERIALIZER = input -> input == null ? null : new String(input, StandardCharsets.UTF_8);

    private static final AtomicLong CONTROLLER_SERVICE_COUNTER = new AtomicLong();

    /** Set by each subclass's container-startup method, before {@link #bootstrapSchema(String)} is called. */
    protected String contactPoint;

    /**
     * Creates the keyspace and the single-partition-key, no-clustering-columns table this service is designed
     * around (see the design notes this module's schema follows). Uses a throwaway bootstrap session, entirely
     * separate from any session {@link CQLDistributedMapCache} itself manages.
     */
    protected static void bootstrapSchema(final String contactPoint) {
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(CQLDistributedMapCache.parseContactPoints(contactPoint).get(0))
                .withLocalDatacenter(LOCAL_DATACENTER)
                .build()) {
            // NetworkTopologyStrategy, not SimpleStrategy: recent ScyllaDB versions enable tablet-based
            // replication by default for new keyspaces, and tablets are only supported with
            // NetworkTopologyStrategy -- SimpleStrategy fails outright ("doesn't support tablet replication").
            // NetworkTopologyStrategy works identically on Cassandra, so this is a single shared statement.
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE
                    + " WITH replication = {'class': 'NetworkTopologyStrategy', '" + LOCAL_DATACENTER + "': 1}");
            session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE
                    + " (" + KEY_COLUMN + " blob PRIMARY KEY, " + VALUE_COLUMN + " blob)");
            // Bucketed-strategy table: bucket is the partition key, the cache key is a clustering column, so
            // multiple keys that hash into the same bucket share a partition and are distinguished by key order.
            session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + BUCKETED_TABLE
                    + " (bucket int, " + KEY_COLUMN + " blob, " + VALUE_COLUMN + " blob, PRIMARY KEY (bucket, " + KEY_COLUMN + "))");
        }
    }

    private static String uniqueKey() {
        return UUID.randomUUID().toString();
    }

    /** Caches created by {@link #initializeCache} during the current test, disabled in {@link #closeCaches()}. */
    private final List<CQLDistributedMapCache> cachesUnderTest = new ArrayList<>();

    private CQLDistributedMapCache initializeCache(final boolean strictRemoval, final String defaultTtl) throws Exception {
        final CQLDistributedMapCache cache = new CQLDistributedMapCache();
        final TestRunner runner = TestRunners.newTestRunner(new NoOpProcessor());
        final String serviceId = "cql-distributedmapcache-" + CONTROLLER_SERVICE_COUNTER.incrementAndGet();
        runner.addControllerService(serviceId, cache);
        runner.setProperty(cache, CQLExecutionService.CONTACT_POINTS, contactPoint);
        runner.setProperty(cache, CQLExecutionService.DATACENTER, LOCAL_DATACENTER);
        runner.setProperty(cache, CQLExecutionService.KEYSPACE, KEYSPACE);
        runner.setProperty(cache, CQLDistributedMapCache.TABLE_NAME, TABLE);
        runner.setProperty(cache, CQLDistributedMapCache.KEY_COLUMN_NAME, KEY_COLUMN);
        runner.setProperty(cache, CQLDistributedMapCache.VALUE_COLUMN_NAME, VALUE_COLUMN);
        runner.setProperty(cache, CQLDistributedMapCache.STRICT_REMOVAL, Boolean.toString(strictRemoval));
        if (defaultTtl != null) {
            runner.setProperty(cache, CQLExecutionService.DEFAULT_TTL, defaultTtl);
        }
        runner.enableControllerService(cache);
        cachesUnderTest.add(cache);
        return cache;
    }

    private CQLDistributedMapCache initializeCache() throws Exception {
        return initializeCache(false, null);
    }

    private CQLDistributedMapCache initializeBucketedCache() throws Exception {
        final CQLDistributedMapCache cache = new CQLDistributedMapCache();
        final TestRunner runner = TestRunners.newTestRunner(new NoOpProcessor());
        final String serviceId = "cql-distributedmapcache-bucketed-" + CONTROLLER_SERVICE_COUNTER.incrementAndGet();
        runner.addControllerService(serviceId, cache);
        runner.setProperty(cache, CQLExecutionService.CONTACT_POINTS, contactPoint);
        runner.setProperty(cache, CQLExecutionService.DATACENTER, LOCAL_DATACENTER);
        runner.setProperty(cache, CQLExecutionService.KEYSPACE, KEYSPACE);
        runner.setProperty(cache, CQLDistributedMapCache.TABLE_NAME, BUCKETED_TABLE);
        runner.setProperty(cache, CQLDistributedMapCache.KEY_COLUMN_NAME, KEY_COLUMN);
        runner.setProperty(cache, CQLDistributedMapCache.VALUE_COLUMN_NAME, VALUE_COLUMN);
        runner.setProperty(cache, CQLDistributedMapCache.PARTITIONING_STRATEGY, CQLDistributedMapCache.PARTITIONING_STRATEGY_BUCKETED);
        runner.setProperty(cache, CQLDistributedMapCache.PARTITION_COUNT, String.valueOf(TEST_PARTITION_COUNT));
        runner.enableControllerService(cache);
        cachesUnderTest.add(cache);
        return cache;
    }

    @AfterEach
    void closeCaches() {
        // Each test enables its own CQLDistributedMapCache instance(s), each owning a real CqlSession against
        // the shared container -- disable them here so @OnDisabled actually closes those sessions, rather than
        // leaking one open session per test for the lifetime of the whole IT run.
        cachesUnderTest.forEach(CQLDistributedMapCache::onDisabled);
        cachesUnderTest.clear();
    }

    @Test
    void testPutAndGetRoundTrip() throws Exception {
        final CQLDistributedMapCache cache = initializeCache();
        final String key = uniqueKey();

        cache.put(key, "hello", STRING_SERIALIZER, STRING_SERIALIZER);

        assertEquals("hello", cache.get(key, STRING_SERIALIZER, STRING_DESERIALIZER));
    }

    @Test
    void testGetReturnsNullForMissingKey() throws Exception {
        final CQLDistributedMapCache cache = initializeCache();

        assertNull(cache.get(uniqueKey(), STRING_SERIALIZER, STRING_DESERIALIZER));
    }

    @Test
    void testContainsKeyReflectsRealPresence() throws Exception {
        final CQLDistributedMapCache cache = initializeCache();
        final String key = uniqueKey();

        assertFalse(cache.containsKey(key, STRING_SERIALIZER));
        cache.put(key, "value", STRING_SERIALIZER, STRING_SERIALIZER);
        assertTrue(cache.containsKey(key, STRING_SERIALIZER));
    }

    @Test
    void testPutIfAbsentOnlySucceedsOnce() throws Exception {
        final CQLDistributedMapCache cache = initializeCache();
        final String key = uniqueKey();

        assertTrue(cache.putIfAbsent(key, "first", STRING_SERIALIZER, STRING_SERIALIZER));
        assertFalse(cache.putIfAbsent(key, "second", STRING_SERIALIZER, STRING_SERIALIZER));
        assertEquals("first", cache.get(key, STRING_SERIALIZER, STRING_DESERIALIZER));
    }

    @Test
    void testGetAndPutIfAbsentReturnsExistingValueFromRealServer() throws Exception {
        // The regression this rewrite fixed: on a real cluster, the second call's failed lightweight
        // transaction must carry back the CURRENT value in the same round trip, not something a mock could
        // paper over -- this is exactly the scenario a unit test against a mocked driver cannot prove.
        final CQLDistributedMapCache cache = initializeCache();
        final String key = uniqueKey();

        assertNull(cache.getAndPutIfAbsent(key, "original", STRING_SERIALIZER, STRING_SERIALIZER, STRING_DESERIALIZER));
        final String existing = cache.getAndPutIfAbsent(key, "attempted-overwrite", STRING_SERIALIZER, STRING_SERIALIZER, STRING_DESERIALIZER);

        assertEquals("original", existing);
        assertEquals("original", cache.get(key, STRING_SERIALIZER, STRING_DESERIALIZER));
    }

    @Test
    void testRemoveBestEffortReflectsExistence() throws Exception {
        final CQLDistributedMapCache cache = initializeCache();
        final String key = uniqueKey();

        assertFalse(cache.remove(key, STRING_SERIALIZER));
        cache.put(key, "value", STRING_SERIALIZER, STRING_SERIALIZER);
        assertTrue(cache.remove(key, STRING_SERIALIZER));
        assertFalse(cache.containsKey(key, STRING_SERIALIZER));
    }

    @Test
    void testRemoveStrictModeReflectsExistence() throws Exception {
        final CQLDistributedMapCache strictCache = initializeCache(true, null);
        final String key = uniqueKey();

        assertFalse(strictCache.remove(key, STRING_SERIALIZER));
        strictCache.put(key, "value", STRING_SERIALIZER, STRING_SERIALIZER);
        assertTrue(strictCache.remove(key, STRING_SERIALIZER));
    }

    @Test
    void testSubMapFetchesMultipleKeysAsynchronously() throws Exception {
        final CQLDistributedMapCache cache = initializeCache();
        final String key1 = uniqueKey();
        final String key2 = uniqueKey();
        cache.put(key1, "v1", STRING_SERIALIZER, STRING_SERIALIZER);
        cache.put(key2, "v2", STRING_SERIALIZER, STRING_SERIALIZER);

        final Map<String, String> results = cache.subMap(Set.of(key1, key2), STRING_SERIALIZER, STRING_DESERIALIZER);

        assertEquals("v1", results.get(key1));
        assertEquals("v2", results.get(key2));
    }

    @Test
    void testBucketedPutAndGetRoundTrip() throws Exception {
        final CQLDistributedMapCache cache = initializeBucketedCache();
        final String key = uniqueKey();

        cache.put(key, "hello", STRING_SERIALIZER, STRING_SERIALIZER);

        assertEquals("hello", cache.get(key, STRING_SERIALIZER, STRING_DESERIALIZER));
        assertTrue(cache.containsKey(key, STRING_SERIALIZER));
    }

    @Test
    void testBucketedPutIfAbsentOnlySucceedsOnceOnRealServer() throws Exception {
        // Proves the compound (bucket, cache_key) lightweight-transaction condition actually works against a
        // real cluster -- a shared bucket partition doesn't let two different keys' conditional inserts, or two
        // conditional inserts of the SAME key, both incorrectly succeed.
        final CQLDistributedMapCache cache = initializeBucketedCache();
        final String key = uniqueKey();

        assertTrue(cache.putIfAbsent(key, "first", STRING_SERIALIZER, STRING_SERIALIZER));
        assertFalse(cache.putIfAbsent(key, "second", STRING_SERIALIZER, STRING_SERIALIZER));
        assertEquals("first", cache.get(key, STRING_SERIALIZER, STRING_DESERIALIZER));

        final String otherKey = uniqueKey();
        assertTrue(cache.putIfAbsent(otherKey, "unrelated", STRING_SERIALIZER, STRING_SERIALIZER));
        assertEquals("unrelated", cache.get(otherKey, STRING_SERIALIZER, STRING_DESERIALIZER));
        assertEquals("first", cache.get(key, STRING_SERIALIZER, STRING_DESERIALIZER));
    }

    @Test
    void testDefaultTtlExpiresEntryOnRealServer() throws Exception {
        final CQLDistributedMapCache cache = initializeCache(false, "1 sec");
        final String key = uniqueKey();

        cache.put(key, "expiring", STRING_SERIALIZER, STRING_SERIALIZER);
        assertEquals("expiring", cache.get(key, STRING_SERIALIZER, STRING_DESERIALIZER));

        Thread.sleep(2500);

        assertNull(cache.get(key, STRING_SERIALIZER, STRING_DESERIALIZER));
    }
}
