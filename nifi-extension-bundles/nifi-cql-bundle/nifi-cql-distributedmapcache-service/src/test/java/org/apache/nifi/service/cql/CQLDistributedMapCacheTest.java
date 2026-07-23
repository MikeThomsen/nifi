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
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CQLDistributedMapCacheTest {

    private static final Serializer<String> STRING_SERIALIZER = (value, output) -> output.write(value.getBytes(StandardCharsets.UTF_8));
    private static final Deserializer<String> STRING_DESERIALIZER = input -> input == null ? null : new String(input, StandardCharsets.UTF_8);

    private CqlSession session;
    private PreparedStatement putPs;
    private PreparedStatement putIfAbsentPs;
    private PreparedStatement fetchPs;
    private PreparedStatement existsPs;
    private PreparedStatement deletePs;

    private CQLDistributedMapCache cache;

    @BeforeEach
    void setUp() {
        session = mock(CqlSession.class);
        putPs = mock(PreparedStatement.class);
        putIfAbsentPs = mock(PreparedStatement.class);
        fetchPs = mock(PreparedStatement.class);
        existsPs = mock(PreparedStatement.class);
        deletePs = mock(PreparedStatement.class);

        when(session.prepare(anyString())).thenAnswer(invocation -> {
            final String cql = invocation.getArgument(0, String.class);
            if (cql.contains("IF NOT EXISTS")) {
                return putIfAbsentPs;
            } else if (cql.startsWith("INSERT")) {
                return putPs;
            } else if (cql.contains("LIMIT 1")) {
                return existsPs;
            } else if (cql.startsWith("SELECT")) {
                return fetchPs;
            } else if (cql.startsWith("DELETE")) {
                return deletePs;
            }
            throw new IllegalArgumentException("Unexpected CQL: " + cql);
        });

        cache = new CQLDistributedMapCache();
        cache.initializeForTesting(session, "cache_table", "cache_key", "cache_value", false, 1024L, null);
    }

    private BoundStatement mockBind(final PreparedStatement preparedStatement) {
        final BoundStatement bound = mock(BoundStatement.class);
        when(preparedStatement.bind(any(), any())).thenReturn(bound);
        when(preparedStatement.bind(any())).thenReturn(bound);
        return bound;
    }

    @Test
    void testPutExecutesPlainInsert() throws IOException {
        final BoundStatement bound = mockBind(putPs);

        cache.put("key", "value", STRING_SERIALIZER, STRING_SERIALIZER);

        verify(session).execute(bound);
    }

    @Test
    void testPutRejectsValueLargerThanMaximumValueSize() {
        cache.initializeForTesting(session, "cache_table", "cache_key", "cache_value", false, 2L, null);

        assertThrows(SerializationException.class,
                () -> cache.put("key", "a value longer than two bytes", STRING_SERIALIZER, STRING_SERIALIZER));

        verify(session, never()).execute(any(com.datastax.oss.driver.api.core.cql.Statement.class));
    }

    @Test
    void testGetReturnsDeserializedValueWhenPresent() throws IOException {
        final BoundStatement bound = mockBind(fetchPs);
        final ResultSet resultSet = mock(ResultSet.class);
        final Row row = mock(Row.class);
        when(session.execute(bound)).thenReturn(resultSet);
        when(resultSet.one()).thenReturn(row);
        when(row.getByteBuffer("cache_value")).thenReturn(ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)));

        final String value = cache.get("key", STRING_SERIALIZER, STRING_DESERIALIZER);

        assertEquals("hello", value);
    }

    @Test
    void testGetReturnsNullWhenAbsent() throws IOException {
        final BoundStatement bound = mockBind(fetchPs);
        final ResultSet resultSet = mock(ResultSet.class);
        when(session.execute(bound)).thenReturn(resultSet);
        when(resultSet.one()).thenReturn(null);

        assertNull(cache.get("key", STRING_SERIALIZER, STRING_DESERIALIZER));
    }

    @Test
    void testContainsKeyUsesExistenceCheckNotValueFetch() throws IOException {
        final BoundStatement bound = mockBind(existsPs);
        final ResultSet resultSet = mock(ResultSet.class);
        final Iterator<Row> iterator = Collections.singletonList(mock(Row.class)).iterator();
        when(session.execute(bound)).thenReturn(resultSet);
        when(resultSet.iterator()).thenReturn(iterator);

        assertTrue(cache.containsKey("key", STRING_SERIALIZER));

        verify(fetchPs, never()).bind(any());
    }

    @Test
    void testContainsKeyReturnsFalseWhenNoRow() throws IOException {
        final BoundStatement bound = mockBind(existsPs);
        final ResultSet resultSet = mock(ResultSet.class);
        when(session.execute(bound)).thenReturn(resultSet);
        when(resultSet.iterator()).thenReturn(Collections.<Row>emptyList().iterator());

        assertFalse(cache.containsKey("key", STRING_SERIALIZER));
    }

    @Test
    void testPutIfAbsentReturnsTrueWhenApplied() throws IOException {
        final BoundStatement bound = mockBind(putIfAbsentPs);
        final ResultSet resultSet = mock(ResultSet.class);
        when(session.execute(bound)).thenReturn(resultSet);
        when(resultSet.wasApplied()).thenReturn(true);

        assertTrue(cache.putIfAbsent("key", "value", STRING_SERIALIZER, STRING_SERIALIZER));
    }

    @Test
    void testPutIfAbsentReturnsFalseWhenKeyAlreadyExists() throws IOException {
        final BoundStatement bound = mockBind(putIfAbsentPs);
        final ResultSet resultSet = mock(ResultSet.class);
        when(session.execute(bound)).thenReturn(resultSet);
        when(resultSet.wasApplied()).thenReturn(false);

        assertFalse(cache.putIfAbsent("key", "value", STRING_SERIALIZER, STRING_SERIALIZER));
    }

    @Test
    void testGetAndPutIfAbsentReturnsNullAndWritesWhenKeyWasAbsent() throws IOException {
        final BoundStatement bound = mockBind(putIfAbsentPs);
        final ResultSet resultSet = mock(ResultSet.class);
        when(session.execute(bound)).thenReturn(resultSet);
        when(resultSet.wasApplied()).thenReturn(true);

        final String result = cache.getAndPutIfAbsent("key", "value", STRING_SERIALIZER, STRING_SERIALIZER, STRING_DESERIALIZER);

        assertNull(result);
        verify(fetchPs, never()).bind(any());
    }

    @Test
    void testGetAndPutIfAbsentReturnsExistingValueFromSingleRoundTrip() throws IOException {
        // Regression test for the bug found in the previous implementation, which did a separate get() before
        // putIfAbsent(), for three total round trips instead of one, and inherited putIfAbsent()'s race condition.
        // Here, the failed conditional INSERT's own result set carries the existing row -- no fetchPs interaction
        // should happen at all.
        final BoundStatement bound = mockBind(putIfAbsentPs);
        final ResultSet resultSet = mock(ResultSet.class);
        final Row existingRow = mock(Row.class);
        when(session.execute(bound)).thenReturn(resultSet);
        when(resultSet.wasApplied()).thenReturn(false);
        when(resultSet.one()).thenReturn(existingRow);
        when(existingRow.getByteBuffer("cache_value")).thenReturn(ByteBuffer.wrap("existing".getBytes(StandardCharsets.UTF_8)));

        final String result = cache.getAndPutIfAbsent("key", "new-value", STRING_SERIALIZER, STRING_SERIALIZER, STRING_DESERIALIZER);

        assertEquals("existing", result);
        verify(fetchPs, never()).bind(any());
    }

    @Test
    void testRemoveBestEffortReflectsPriorExistence() throws IOException {
        final BoundStatement existsBound = mockBind(existsPs);
        final BoundStatement deleteBound = mockBind(deletePs);
        final ResultSet existsResultSet = mock(ResultSet.class);
        when(session.execute(existsBound)).thenReturn(existsResultSet);
        when(existsResultSet.iterator()).thenReturn(Collections.<Row>emptyList().iterator());

        assertFalse(cache.remove("key", STRING_SERIALIZER));

        verify(session).execute(deleteBound);
    }

    @Test
    void testRemoveStrictModeReturnsWhetherDeleteWasApplied() throws IOException {
        cache.initializeForTesting(session, "cache_table", "cache_key", "cache_value", true, 1024L, null);
        final BoundStatement deleteBound = mockBind(deletePs);
        final ResultSet resultSet = mock(ResultSet.class);
        when(session.execute(deleteBound)).thenReturn(resultSet);
        when(resultSet.wasApplied()).thenReturn(true);

        assertTrue(cache.remove("key", STRING_SERIALIZER));
        verify(existsPs, never()).bind(any());
    }

    @Test
    void testSubMapFetchesAllKeysAsynchronously() throws IOException {
        final BoundStatement bound = mockBind(fetchPs);
        final AsyncResultSet asyncResultSet = mock(AsyncResultSet.class);
        final Row row = mock(Row.class);
        when(session.executeAsync(bound)).thenReturn(CompletableFuture.completedFuture(asyncResultSet));
        when(asyncResultSet.one()).thenReturn(row);
        when(row.getByteBuffer("cache_value")).thenReturn(ByteBuffer.wrap("v".getBytes(StandardCharsets.UTF_8)));

        final Map<String, String> results = cache.subMap(Set.of("k1", "k2"), STRING_SERIALIZER, STRING_DESERIALIZER);

        assertEquals(2, results.size());
        assertEquals("v", results.get("k1"));
        assertEquals("v", results.get("k2"));
    }

    @Test
    void testSubMapReturnsNullForKeysNotFound() throws IOException {
        final BoundStatement bound = mockBind(fetchPs);
        final AsyncResultSet asyncResultSet = mock(AsyncResultSet.class);
        when(session.executeAsync(bound)).thenReturn(CompletableFuture.completedFuture(asyncResultSet));
        when(asyncResultSet.one()).thenReturn(null);

        final Map<String, String> results = cache.subMap(Set.of("missing"), STRING_SERIALIZER, STRING_DESERIALIZER);

        assertNull(results.get("missing"));
        assertTrue(results.containsKey("missing"));
    }

    @Test
    void testSerializerExceptionPropagates() {
        final Serializer<String> failingSerializer = (value, output) -> {
            throw new IOException("boom");
        };

        assertThrows(IOException.class, () -> cache.put("key", "value", failingSerializer, STRING_SERIALIZER));
    }

    // ---- Bucketed partitioning strategy ----

    @Test
    void testPutInBucketedModeBindsComputedBucketKeyAndValue() throws IOException {
        cache.initializeForTesting(session, "cache_table", "cache_key", "cache_value", false, 1024L, null, true, 100);
        final BoundStatement bound = mock(BoundStatement.class);
        when(putPs.bind(any(), any(), any())).thenReturn(bound);

        cache.put("key", "value", STRING_SERIALIZER, STRING_SERIALIZER);

        final ArgumentCaptor<Object> args = ArgumentCaptor.forClass(Object.class);
        verify(putPs).bind(args.capture(), args.capture(), args.capture());
        final List<Object> captured = args.getAllValues();
        final int expectedBucket = CQLDistributedMapCache.computeBucket("key".getBytes(StandardCharsets.UTF_8), 100);
        assertEquals(expectedBucket, captured.get(0));
        assertEquals(ByteBuffer.wrap("key".getBytes(StandardCharsets.UTF_8)), captured.get(1));
        assertEquals(ByteBuffer.wrap("value".getBytes(StandardCharsets.UTF_8)), captured.get(2));
        verify(session).execute(bound);
    }

    @Test
    void testGetInBucketedModeBindsComputedBucketAndKey() throws IOException {
        cache.initializeForTesting(session, "cache_table", "cache_key", "cache_value", false, 1024L, null, true, 100);
        final BoundStatement bound = mock(BoundStatement.class);
        when(fetchPs.bind(any(), any())).thenReturn(bound);
        final ResultSet resultSet = mock(ResultSet.class);
        final Row row = mock(Row.class);
        when(session.execute(bound)).thenReturn(resultSet);
        when(resultSet.one()).thenReturn(row);
        when(row.getByteBuffer("cache_value")).thenReturn(ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)));

        final String value = cache.get("key", STRING_SERIALIZER, STRING_DESERIALIZER);

        assertEquals("hello", value);
        final ArgumentCaptor<Object> args = ArgumentCaptor.forClass(Object.class);
        verify(fetchPs).bind(args.capture(), args.capture());
        final int expectedBucket = CQLDistributedMapCache.computeBucket("key".getBytes(StandardCharsets.UTF_8), 100);
        assertEquals(expectedBucket, args.getAllValues().get(0));
        assertEquals(ByteBuffer.wrap("key".getBytes(StandardCharsets.UTF_8)), args.getAllValues().get(1));
    }

    @Test
    void testPutIfAbsentInBucketedModeStillHonorsWasApplied() throws IOException {
        cache.initializeForTesting(session, "cache_table", "cache_key", "cache_value", false, 1024L, null, true, 100);
        final BoundStatement bound = mock(BoundStatement.class);
        when(putIfAbsentPs.bind(any(), any(), any())).thenReturn(bound);
        final ResultSet resultSet = mock(ResultSet.class);
        when(session.execute(bound)).thenReturn(resultSet);
        when(resultSet.wasApplied()).thenReturn(false);

        assertFalse(cache.putIfAbsent("key", "value", STRING_SERIALIZER, STRING_SERIALIZER));
    }
}
