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
package org.apache.nifi.service.cql.cache.it;

import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.service.cql.api.service.QueryOverrides;
import org.apache.nifi.service.cql.cache.CQLDistributedMapCache;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs {@link CQLDistributedMapCache} against a real cluster - Cassandra or ScyllaDB, whichever profile is
 * active.
 *
 * <p>This class names no backend type. The session provider's class name and the container image come from
 * system properties the active profile sets, and the provider is instantiated reflectively, so one source
 * file compiles against either profile's classpath. That is the price of keeping the two backends'
 * dependencies apart without splitting the sources: a wrong class name is a runtime failure rather than a
 * compile error, which is why {@link #newSessionProvider()} reports what it was asked for when it cannot
 * find it.
 *
 * <p>Bootstrapping runs through {@code cqlsh} inside the container rather than through a driver, for the same
 * reason - there is no driver this class is allowed to see. It doubles as the readiness probe: the container
 * is ready when cqlsh can answer, which is a better signal than the CQL port accepting connections.
 *
 * <p>The cache's unit tests already cover every operation against an in-memory stand-in, faster and in more
 * combinations. What only a real cluster settles is whether the CQL the cache builds is valid, and whether
 * the lightweight transactions behind {@code putIfAbsent} and friends behave as the abstraction claims.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DistributedMapCacheIT {

    private static final String KEYSPACE = "cachespace";
    private static final String TABLE = "cache";
    private static final String LOCAL_DATACENTER = "datacenter1";
    private static final int CQL_PORT = 9042;

    /** How long to keep retrying cqlsh before giving up on the container ever becoming usable. */
    private static final Duration READINESS_TIMEOUT = Duration.ofMinutes(3);

    private static final Serializer<String> STRING_SERIALIZER =
            (value, out) -> out.write(value.getBytes(StandardCharsets.UTF_8));
    private static final Deserializer<String> STRING_DESERIALIZER =
            input -> input == null ? null : new String(input, StandardCharsets.UTF_8);

    /** Only here so the TestRunner has something to hang controller services off. */
    public static class NoOpProcessor extends AbstractProcessor {
        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) {
        }
    }

    private GenericContainer<?> container;
    private CQLDistributedMapCache cache;

    @BeforeAll
    void setUp() throws Exception {
        final String image = requiredProperty("dmc.image");

        container = new GenericContainer<>(image)
                .withExposedPorts(CQL_PORT)
                .waitingFor(Wait.forListeningPort())
                .withStartupTimeout(READINESS_TIMEOUT);
        container.start();

        awaitCql();
        execCql("create keyspace if not exists " + KEYSPACE + " with replication = "
                + "{'class': 'NetworkTopologyStrategy', '" + LOCAL_DATACENTER + "': 1}");

        final TestRunner runner = TestRunners.newTestRunner(new NoOpProcessor());

        final CQLExecutionService executionService = newSessionProvider();
        runner.addControllerService("cql", executionService);
        runner.setProperty(executionService, CQLExecutionService.CONTACT_POINTS,
                container.getHost() + ":" + container.getMappedPort(CQL_PORT));
        runner.setProperty(executionService, CQLExecutionService.DATACENTER, LOCAL_DATACENTER);
        runner.setProperty(executionService, CQLExecutionService.KEYSPACE, KEYSPACE);
        runner.enableControllerService(executionService);

        // Created through the service rather than through cqlsh, so both backends get byte-identical schema
        // and the DDL exercises execute() on the way past.
        executionService.execute("create table if not exists " + KEYSPACE + "." + TABLE
                + " (cache_key blob primary key, cache_value blob)", List.of(), QueryOverrides.NONE);

        cache = new CQLDistributedMapCache();
        runner.addControllerService("cache", cache);
        runner.setProperty(cache, CQLDistributedMapCache.CQL_EXECUTION_SERVICE, "cql");
        runner.setProperty(cache, CQLDistributedMapCache.TABLE_NAME, KEYSPACE + "." + TABLE);
        runner.setProperty(cache, CQLDistributedMapCache.STRICT_REMOVAL, "true");
        runner.enableControllerService(cache);
    }

    @AfterAll
    void tearDown() {
        if (container != null) {
            container.stop();
        }
    }

    private static String requiredProperty(final String name) {
        final String value = System.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("System property '" + name + "' is not set. This test is driven by "
                    + "the cassandra or scylladb profile; run it with -Pintegration-tests and one of those.");
        }
        return value;
    }

    private static CQLExecutionService newSessionProvider() {
        final String className = requiredProperty("dmc.provider");

        try {
            return (CQLExecutionService) Class.forName(className).getDeclaredConstructor().newInstance();
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException("Could not instantiate the session provider '" + className
                    + "'. The active profile must put the module supplying it on the test classpath.", e);
        }
    }

    /**
     * Waits until cqlsh can execute a trivial statement. The CQL port starts listening well before the node
     * will answer a query, so waiting on the port alone produces a container that accepts a connection and
     * then fails the first statement - especially on ScyllaDB, which settles schema through Raft after boot.
     */
    private void awaitCql() throws Exception {
        final long deadline = System.nanoTime() + READINESS_TIMEOUT.toNanos();
        Exception lastFailure = null;

        while (System.nanoTime() < deadline) {
            try {
                execCql("select release_version from system.local");
                return;
            } catch (final Exception e) {
                lastFailure = e;
                Thread.sleep(2_000);
            }
        }

        throw new IllegalStateException("Container never became ready for CQL within " + READINESS_TIMEOUT,
                lastFailure);
    }

    private void execCql(final String cql) throws Exception {
        final Container.ExecResult result = container.execInContainer("cqlsh", "-e", cql);

        if (result.getExitCode() != 0) {
            throw new IllegalStateException("cqlsh failed (" + result.getExitCode() + ") for [" + cql + "]: "
                    + result.getStderr());
        }
    }

    /** A key unique to this test method, so one test cannot observe another's entries. */
    private static String key(final String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    private void put(final String key, final String value) throws IOException {
        cache.put(key, value, STRING_SERIALIZER, STRING_SERIALIZER);
    }

    private String get(final String key) throws IOException {
        return cache.get(key, STRING_SERIALIZER, STRING_DESERIALIZER);
    }

    // ---- Unconditional operations --------------------------------------------------------------------

    @Test
    @DisplayName("A value round-trips through a real blob column")
    void testPutAndGet() throws IOException {
        final String key = key("rt");

        put(key, "hello");

        assertEquals("hello", get(key));
        assertNull(get(key("never-written")));
    }

    @Test
    @DisplayName("A second put overwrites, since an unconditional CQL write is an upsert")
    void testPutOverwrites() throws IOException {
        final String key = key("ow");

        put(key, "first");
        put(key, "second");

        assertEquals("second", get(key));
    }

    @Test
    @DisplayName("containsKey answers from the server without transferring the value")
    void testContainsKey() throws IOException {
        final String key = key("ck");

        assertFalse(cache.containsKey(key, STRING_SERIALIZER));
        put(key, "v");
        assertTrue(cache.containsKey(key, STRING_SERIALIZER));
    }

    // ---- Conditional operations, which only a real cluster can settle --------------------------------

    @Test
    @DisplayName("putIfAbsent is decided by the cluster, so only the first caller creates the entry")
    void testPutIfAbsent() throws IOException {
        final String key = key("pia");

        assertTrue(cache.putIfAbsent(key, "first", STRING_SERIALIZER, STRING_SERIALIZER));
        assertFalse(cache.putIfAbsent(key, "second", STRING_SERIALIZER, STRING_SERIALIZER));

        assertEquals("first", get(key), "the losing write must not have landed");
    }

    @Test
    @DisplayName("getAndPutIfAbsent returns the stored value carried back by the rejected write itself")
    void testGetAndPutIfAbsent() throws IOException {
        final String key = key("gpia");

        assertNull(cache.getAndPutIfAbsent(key, "original", STRING_SERIALIZER, STRING_SERIALIZER, STRING_DESERIALIZER),
                "creating the entry reports no prior value");

        final String existing =
                cache.getAndPutIfAbsent(key, "attempted", STRING_SERIALIZER, STRING_SERIALIZER, STRING_DESERIALIZER);

        assertEquals("original", existing);
        assertEquals("original", get(key));
    }

    @Test
    @DisplayName("Removal reports whether the entry was there, and it is gone afterwards")
    void testRemove() throws IOException {
        final String key = key("rm");
        put(key, "v");

        assertTrue(cache.remove(key, STRING_SERIALIZER));
        assertNull(get(key));
        assertFalse(cache.remove(key, STRING_SERIALIZER));
    }

    @Test
    @DisplayName("removeAndGet returns the value it deleted, having read it first - CQL has no delete-and-return")
    void testRemoveAndGet() throws IOException {
        final String key = key("rag");
        put(key, "doomed");

        assertEquals("doomed", cache.removeAndGet(key, STRING_SERIALIZER, STRING_DESERIALIZER));
        assertNull(get(key));
        assertNull(cache.removeAndGet(key, STRING_SERIALIZER, STRING_DESERIALIZER),
                "removing an entry that is already gone yields no value");
    }

    // ---- Bulk operations -----------------------------------------------------------------------------

    @Test
    @DisplayName("subMap returns a value per requested key, null for those absent")
    void testSubMap() throws IOException {
        final String present = key("sm-a");
        final String absent = key("sm-b");
        put(present, "1");

        final Map<String, String> values =
                cache.subMap(Set.of(present, absent), STRING_SERIALIZER, STRING_DESERIALIZER);

        assertEquals("1", values.get(present));
        assertNull(values.get(absent));
        assertTrue(values.containsKey(absent), "an absent key is present in the map with a null value");
    }

    @Test
    @DisplayName("putAll writes every entry")
    void testPutAll() throws IOException {
        final String first = key("pa-1");
        final String second = key("pa-2");

        cache.putAll(Map.of(first, "1", second, "2"), STRING_SERIALIZER, STRING_SERIALIZER);

        assertEquals("1", get(first));
        assertEquals("2", get(second));
    }

    // ---- The property that makes a blob column the right choice ---------------------------------------

    @Test
    @DisplayName("Arbitrary bytes survive unchanged, including nulls and the full signed range")
    void testArbitraryBytesRoundTrip() throws IOException {
        final String key = key("bytes");
        final byte[] awkward = {0, -1, 65, 0, 127, -128};
        final Serializer<byte[]> raw = (value, out) -> out.write(value);
        final Deserializer<byte[]> rawBack = input -> input;

        cache.put(key, awkward, STRING_SERIALIZER, raw);

        assertArrayEquals(awkward, cache.get(key, STRING_SERIALIZER, rawBack));
    }
}
