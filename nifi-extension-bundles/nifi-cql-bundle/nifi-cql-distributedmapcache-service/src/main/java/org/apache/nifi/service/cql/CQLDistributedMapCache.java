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
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.service.cql.api.CQLExecutionService;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.zip.CRC32;

/**
 * A {@link DistributedMapCacheClient} backed by Apache Cassandra or ScyllaDB. By default (Partitioning Strategy =
 * Simple Key/Value), each cache entry is stored as a single row keyed by its own partition key in an
 * operator-managed table -- one partition per key, no clustering columns -- which is the idiomatic, safest schema
 * shape for this access pattern and the right choice for almost all deployments. An opt-in Bucketed strategy is
 * also available for a specific, narrow situation: a very large number of cache entries on a small or
 * cost-constrained cluster, where per-partition memory overhead (bloom filters, indexes) becomes the binding
 * constraint rather than raw data volume. See {@link #PARTITIONING_STRATEGY} for the trade-offs.
 *
 * <p>The table's key and value columns must both be of CQL type {@code blob}: {@link Serializer}/{@link Deserializer}
 * erase the caller's original Java type into opaque bytes before they ever reach this class, so {@code blob} is the
 * only column type that can safely hold them without attempting to reinterpret an encoding this class has no
 * contract to understand.
 */
@Tags({"cassandra", "scylladb", "map", "cache", "distributed"})
@CapabilityDescription("Provides a DistributedMapCacheClient backed by Apache Cassandra or ScyllaDB. Each cache entry is stored "
        + "as a single row in an existing table, keyed by its own partition key with no clustering columns by default, or "
        + "grouped into a fixed number of hash-bucketed partitions when the optional Bucketed partitioning strategy is enabled.")
public class CQLDistributedMapCache extends AbstractControllerService implements DistributedMapCacheClient, VerifiableControllerService {

    static final int DEFAULT_CQL_PORT = 9042;

    /** Fixed name of the bucket column used when {@link #PARTITIONING_STRATEGY} is {@link #PARTITIONING_STRATEGY_BUCKETED}. */
    static final String BUCKET_COLUMN = "bucket";

    public static final AllowableValue PARTITIONING_STRATEGY_SIMPLE = new AllowableValue(
            "simple", "Simple Key/Value",
            "Each cache key is its own partition (no clustering columns). This is the idiomatic, safest Cassandra/"
                    + "ScyllaDB schema for this access pattern and should be the default choice for almost all deployments.");

    public static final AllowableValue PARTITIONING_STRATEGY_BUCKETED = new AllowableValue(
            "bucketed", "Bucketed",
            "Cache keys are hashed into a fixed number of shared partitions (see Partition Count), using the key as a "
                    + "clustering column within its assigned partition. WARNING: only use this if you have a very large "
                    + "number of cache entries (hundreds of millions or more) on a small or cost-constrained cluster where "
                    + "per-partition memory overhead, not data volume, is the binding constraint -- for example, a "
                    + "deliberately cheap non-production tier that still needs to hold a realistic, large dataset. This "
                    + "trades that memory overhead for a real cost: conditional operations (putIfAbsent, "
                    + "getAndPutIfAbsent, and any compare-and-swap) on different keys that happen to hash into the same "
                    + "partition will contend with each other, since Cassandra/ScyllaDB serialize lightweight "
                    + "transactions per partition, not per row. The Partition Count also cannot be safely changed once "
                    + "data has been written without migrating the entire dataset. For typical deployments, Simple "
                    + "Key/Value is safer and simpler.");

    public static final PropertyDescriptor PARTITIONING_STRATEGY = new PropertyDescriptor.Builder()
            .name("Partitioning Strategy")
            .description("Controls how cache entries are mapped to partitions. See the allowable value descriptions, "
                    + "particularly for Bucketed, before changing this from the default.")
            .required(true)
            .allowableValues(PARTITIONING_STRATEGY_SIMPLE, PARTITIONING_STRATEGY_BUCKETED)
            .defaultValue(PARTITIONING_STRATEGY_SIMPLE.getValue())
            .build();

    public static final PropertyDescriptor PARTITION_COUNT = new PropertyDescriptor.Builder()
            .name("Partition Count")
            .description("The fixed number of hash buckets to spread cache entries across when Partitioning Strategy "
                    + "is Bucketed. Choose this based on your expected total key count and average value size so that "
                    + "no bucket's total size grows beyond a safe partition size (aim for tens of MB, not GB -- see the "
                    + "design documentation for the sizing formula). When in doubt, prefer a larger value within the "
                    + "allowed range: the memory overhead of additional partitions is minor once bucketing is in use at "
                    + "all, while undersizing risks oversized partitions that can only be fixed by migrating the entire "
                    + "dataset to a new Partition Count. The allowed range (32,768 to 1,000,000,000) is sized for "
                    + "clusters from a handful of nodes up through a small production-scale cluster: the lower bound "
                    + "keeps entries spread across enough partitions to avoid concentrating load on any single node's "
                    + "token ranges even on a small cluster, and the upper bound keeps aggregate bloom filter/index "
                    + "memory overhead in the low hundreds of MB per node even at the ceiling.")
            .required(true)
            .defaultValue("1000000")
            .addValidator(StandardValidators.createLongValidator(32_768L, 1_000_000_000L, true))
            .dependsOn(PARTITIONING_STRATEGY, PARTITIONING_STRATEGY_BUCKETED)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the table backing the cache. The table is not created by this service and must already "
                    + "exist, with a schema matching the Key Column Name and Value Column Name properties below, and -- when "
                    + "Partitioning Strategy is Bucketed -- an additional 'bucket' int column as the partition key.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_COLUMN_NAME = new PropertyDescriptor.Builder()
            .name("Key Column Name")
            .description("The name of the column that stores the cache key, of CQL type 'blob'. When Partitioning "
                    + "Strategy is Simple Key/Value, this column must be the table's sole partition key -- one row per "
                    + "key, one partition per row, with no clustering columns. When Partitioning Strategy is Bucketed, "
                    + "this column must instead be a clustering column following the 'bucket' partition key column.")
            .required(true)
            .defaultValue("cache_key")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor VALUE_COLUMN_NAME = new PropertyDescriptor.Builder()
            .name("Value Column Name")
            .description("The name of the column that stores the cache value. This column must be of CQL type 'blob'.")
            .required(true)
            .defaultValue("cache_value")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor STRICT_REMOVAL = new PropertyDescriptor.Builder()
            .name("Strict Removal")
            .description("If true, remove() pays for a conditional 'DELETE ... IF EXISTS' so its returned boolean is "
                    + "authoritative under concurrent access. If false (default), remove() performs a lightweight existence "
                    + "check followed by a plain delete, which is cheaper but leaves a small race window between the two "
                    + "statements -- acceptable for callers that don't treat the returned boolean as load-bearing.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor MAX_VALUE_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum Value Size")
            .description("The maximum permitted size of a serialized cache value. Values larger than this are rejected "
                    + "before being written, to guard against accidentally writing oversized single-partition rows.")
            .required(true)
            .defaultValue("1 MB")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CQLExecutionService.CONTACT_POINTS,
            CQLExecutionService.DATACENTER,
            CQLExecutionService.KEYSPACE,
            CQLExecutionService.PROP_SSL_CONTEXT_SERVICE,
            CQLExecutionService.USERNAME,
            CQLExecutionService.PASSWORD,
            CQLExecutionService.CONSISTENCY_LEVEL,
            CQLExecutionService.COMPRESSION_TYPE,
            CQLExecutionService.READ_TIMEOUT,
            CQLExecutionService.CONNECT_TIMEOUT,
            CQLExecutionService.DRIVER_CONFIGURATION_FILE,
            CQLExecutionService.DEFAULT_TTL,
            PARTITIONING_STRATEGY,
            PARTITION_COUNT,
            TABLE_NAME,
            KEY_COLUMN_NAME,
            VALUE_COLUMN_NAME,
            STRICT_REMOVAL,
            MAX_VALUE_SIZE
    );

    private CqlSession session;
    private String tableName;
    private String keyColumn;
    private String valueColumn;
    private boolean strictRemoval;
    private long maxValueSizeBytes;
    private boolean bucketed;
    private int partitionCount;

    private PreparedStatement putStatement;
    private PreparedStatement putIfAbsentStatement;
    private PreparedStatement fetchStatement;
    private PreparedStatement existsStatement;
    private PreparedStatement deleteStatement;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
        keyColumn = context.getProperty(KEY_COLUMN_NAME).evaluateAttributeExpressions().getValue();
        valueColumn = context.getProperty(VALUE_COLUMN_NAME).evaluateAttributeExpressions().getValue();
        strictRemoval = context.getProperty(STRICT_REMOVAL).asBoolean();
        maxValueSizeBytes = context.getProperty(MAX_VALUE_SIZE).asDataSize(DataUnit.B).longValue();
        bucketed = PARTITIONING_STRATEGY_BUCKETED.getValue().equals(context.getProperty(PARTITIONING_STRATEGY).getValue());
        partitionCount = bucketed ? context.getProperty(PARTITION_COUNT).asInteger() : 0;

        session = buildSession(context);
        prepareStatements(resolveTtlSeconds(context));
    }

    @OnDisabled
    public void onDisabled() {
        if (session != null) {
            session.close();
        }
        session = null;
        putStatement = null;
        putIfAbsentStatement = null;
        fetchStatement = null;
        existsStatement = null;
        deleteStatement = null;
    }

    /**
     * Package-private seam for unit tests: injects a (typically mocked) {@link CqlSession} and configuration
     * directly, bypassing {@link ConfigurationContext} property resolution, so the {@link DistributedMapCacheClient}
     * method implementations can be unit tested without a live cluster.
     */
    void initializeForTesting(final CqlSession session, final String tableName, final String keyColumn, final String valueColumn,
                               final boolean strictRemoval, final long maxValueSizeBytes, final Integer ttlSeconds) {
        initializeForTesting(session, tableName, keyColumn, valueColumn, strictRemoval, maxValueSizeBytes, ttlSeconds, false, 0);
    }

    /** As above, additionally selecting the Bucketed partitioning strategy with the given partition count. */
    void initializeForTesting(final CqlSession session, final String tableName, final String keyColumn, final String valueColumn,
                               final boolean strictRemoval, final long maxValueSizeBytes, final Integer ttlSeconds,
                               final boolean bucketed, final int partitionCount) {
        this.session = session;
        this.tableName = tableName;
        this.keyColumn = keyColumn;
        this.valueColumn = valueColumn;
        this.strictRemoval = strictRemoval;
        this.maxValueSizeBytes = maxValueSizeBytes;
        this.bucketed = bucketed;
        this.partitionCount = partitionCount;
        prepareStatements(ttlSeconds);
    }

    private void prepareStatements(final Integer ttlSeconds) {
        putStatement = session.prepare(buildPutCql(tableName, keyColumn, valueColumn, ttlSeconds, bucketed));
        putIfAbsentStatement = session.prepare(buildPutIfAbsentCql(tableName, keyColumn, valueColumn, ttlSeconds, bucketed));
        fetchStatement = session.prepare(buildFetchCql(tableName, keyColumn, valueColumn, bucketed));
        existsStatement = session.prepare(buildExistsCql(tableName, keyColumn, bucketed));
        deleteStatement = session.prepare(strictRemoval
                ? buildStrictDeleteCql(tableName, keyColumn, bucketed)
                : buildDeleteCql(tableName, keyColumn, bucketed));
    }

    private Integer resolveTtlSeconds(final ConfigurationContext context) {
        final PropertyValue ttlProperty = context.getProperty(CQLExecutionService.DEFAULT_TTL).evaluateAttributeExpressions();
        return ttlProperty.isSet() ? Math.toIntExact(ttlProperty.asDuration().toSeconds()) : null;
    }

    private CqlSession buildSession(final ConfigurationContext context) {
        final String consistencyLevel = context.getProperty(CQLExecutionService.CONSISTENCY_LEVEL).getValue();
        final String compression = context.getProperty(CQLExecutionService.COMPRESSION_TYPE).getValue();
        final String contactPointList = context.getProperty(CQLExecutionService.CONTACT_POINTS).evaluateAttributeExpressions().getValue();
        final List<InetSocketAddress> contactPoints = parseContactPoints(contactPointList);

        final SSLContextService sslService =
                context.getProperty(CQLExecutionService.PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final SSLContext sslContext = sslService == null ? null : sslService.createContext();

        final PropertyValue usernameProperty = context.getProperty(CQLExecutionService.USERNAME).evaluateAttributeExpressions();
        final PropertyValue passwordProperty = context.getProperty(CQLExecutionService.PASSWORD).evaluateAttributeExpressions();
        final String username = usernameProperty.isSet() ? usernameProperty.getValue() : null;
        final String password = passwordProperty.isSet() ? passwordProperty.getValue() : null;

        final Duration readTimeout = context.getProperty(CQLExecutionService.READ_TIMEOUT).evaluateAttributeExpressions().asDuration();
        final Duration connectTimeout = context.getProperty(CQLExecutionService.CONNECT_TIMEOUT).evaluateAttributeExpressions().asDuration();
        final String datacenter = context.getProperty(CQLExecutionService.DATACENTER).evaluateAttributeExpressions().getValue();
        final String sessionKeyspace = context.getProperty(CQLExecutionService.KEYSPACE).evaluateAttributeExpressions().getValue();

        final DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, connectTimeout)
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, readTimeout)
                .withString(DefaultDriverOption.REQUEST_CONSISTENCY, consistencyLevel)
                .withString(DefaultDriverOption.PROTOCOL_COMPRESSION, compression)
                .build();

        CqlSessionBuilder builder = CqlSession.builder().addContactPoints(contactPoints);
        if (username != null && password != null) {
            builder = builder.withAuthCredentials(username, password);
        }

        return builder
                .withSslContext(sslContext)
                .withLocalDatacenter(datacenter)
                .withKeyspace(sessionKeyspace)
                .withConfigLoader(loader)
                .build();
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger, final Map<String, String> variables) {
        final List<ConfigVerificationResult> results = new ArrayList<>();
        final String datacenter = context.getProperty(CQLExecutionService.DATACENTER).evaluateAttributeExpressions().getValue();

        try (CqlSession verificationSession = buildSession(context)) {
            verificationSession.execute("SELECT release_version FROM system.local");
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Establish Connection")
                    .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                    .explanation("Successfully connected to Cassandra using datacenter [" + datacenter + "]")
                    .build());
        } catch (final Exception e) {
            verificationLogger.warn("Failed to establish Cassandra connection using datacenter [{}]: {}", datacenter, e.getMessage());
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Establish Connection")
                    .outcome(ConfigVerificationResult.Outcome.FAILED)
                    .explanation("Failed to connect using datacenter [" + datacenter + "]: " + e.getMessage())
                    .build());
        }

        return results;
    }

    // ---- CQL statement text, built once at @OnEnabled time and independently unit-testable ----

    static String buildPutCql(final String table, final String keyColumn, final String valueColumn, final Integer ttlSeconds, final boolean bucketed) {
        Insert insert = bucketed
                ? QueryBuilder.insertInto(table)
                        .value(BUCKET_COLUMN, QueryBuilder.bindMarker())
                        .value(keyColumn, QueryBuilder.bindMarker())
                        .value(valueColumn, QueryBuilder.bindMarker())
                : QueryBuilder.insertInto(table)
                        .value(keyColumn, QueryBuilder.bindMarker())
                        .value(valueColumn, QueryBuilder.bindMarker());
        if (ttlSeconds != null) {
            insert = insert.usingTtl(ttlSeconds);
        }
        return insert.asCql();
    }

    static String buildPutIfAbsentCql(final String table, final String keyColumn, final String valueColumn, final Integer ttlSeconds, final boolean bucketed) {
        Insert insert = bucketed
                ? QueryBuilder.insertInto(table)
                        .value(BUCKET_COLUMN, QueryBuilder.bindMarker())
                        .value(keyColumn, QueryBuilder.bindMarker())
                        .value(valueColumn, QueryBuilder.bindMarker())
                : QueryBuilder.insertInto(table)
                        .value(keyColumn, QueryBuilder.bindMarker())
                        .value(valueColumn, QueryBuilder.bindMarker());
        if (ttlSeconds != null) {
            insert = insert.usingTtl(ttlSeconds);
        }
        return insert.ifNotExists().asCql();
    }

    static String buildFetchCql(final String table, final String keyColumn, final String valueColumn, final boolean bucketed) {
        return bucketed
                ? QueryBuilder.selectFrom(table)
                        .column(valueColumn)
                        .whereColumn(BUCKET_COLUMN).isEqualTo(QueryBuilder.bindMarker())
                        .whereColumn(keyColumn).isEqualTo(QueryBuilder.bindMarker())
                        .asCql()
                : QueryBuilder.selectFrom(table)
                        .column(valueColumn)
                        .whereColumn(keyColumn).isEqualTo(QueryBuilder.bindMarker())
                        .asCql();
    }

    static String buildExistsCql(final String table, final String keyColumn, final boolean bucketed) {
        return bucketed
                ? QueryBuilder.selectFrom(table)
                        .column(keyColumn)
                        .whereColumn(BUCKET_COLUMN).isEqualTo(QueryBuilder.bindMarker())
                        .whereColumn(keyColumn).isEqualTo(QueryBuilder.bindMarker())
                        .limit(1)
                        .asCql()
                : QueryBuilder.selectFrom(table)
                        .column(keyColumn)
                        .whereColumn(keyColumn).isEqualTo(QueryBuilder.bindMarker())
                        .limit(1)
                        .asCql();
    }

    static String buildDeleteCql(final String table, final String keyColumn, final boolean bucketed) {
        return bucketed
                ? QueryBuilder.deleteFrom(table)
                        .whereColumn(BUCKET_COLUMN).isEqualTo(QueryBuilder.bindMarker())
                        .whereColumn(keyColumn).isEqualTo(QueryBuilder.bindMarker())
                        .asCql()
                : QueryBuilder.deleteFrom(table)
                        .whereColumn(keyColumn).isEqualTo(QueryBuilder.bindMarker())
                        .asCql();
    }

    static String buildStrictDeleteCql(final String table, final String keyColumn, final boolean bucketed) {
        return bucketed
                ? QueryBuilder.deleteFrom(table)
                        .whereColumn(BUCKET_COLUMN).isEqualTo(QueryBuilder.bindMarker())
                        .whereColumn(keyColumn).isEqualTo(QueryBuilder.bindMarker())
                        .ifExists()
                        .asCql()
                : QueryBuilder.deleteFrom(table)
                        .whereColumn(keyColumn).isEqualTo(QueryBuilder.bindMarker())
                        .ifExists()
                        .asCql();
    }

    /**
     * Assigns a key to a bucket via CRC32 of its serialized bytes, modulo the configured partition count. CRC32 is
     * used rather than a cryptographic hash purely for uniform distribution across buckets -- no security property
     * is needed here -- and is available without adding a dependency.
     */
    static int computeBucket(final byte[] keyBytes, final int partitionCount) {
        final CRC32 crc32 = new CRC32();
        crc32.update(keyBytes);
        return (int) Math.floorMod(crc32.getValue(), (long) partitionCount);
    }

    static List<InetSocketAddress> parseContactPoints(final String contactPointList) {
        final List<InetSocketAddress> contactPoints = new ArrayList<>();
        if (contactPointList == null) {
            return contactPoints;
        }

        for (final String contactPointEntry : contactPointList.split(",")) {
            final String[] addresses = contactPointEntry.split(":");
            final String hostName = addresses[0].trim();
            final int port = addresses.length > 1 ? Integer.parseInt(addresses[1].trim()) : DEFAULT_CQL_PORT;
            contactPoints.add(new InetSocketAddress(hostName, port));
        }

        return contactPoints;
    }

    // ---- DistributedMapCacheClient ----

    @Override
    public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        final byte[] valueBytes = serialize(value, valueSerializer);
        checkValueSize(valueBytes);
        session.execute(bindKeyAndValue(putStatement, serialize(key, keySerializer), valueBytes));
    }

    @Override
    public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        final byte[] valueBytes = serialize(value, valueSerializer);
        checkValueSize(valueBytes);
        final ResultSet resultSet = session.execute(bindKeyAndValue(putIfAbsentStatement, serialize(key, keySerializer), valueBytes));
        return resultSet.wasApplied();
    }

    @Override
    public <K, V> V getAndPutIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer,
                                       final Deserializer<V> valueDeserializer) throws IOException {
        final byte[] valueBytes = serialize(value, valueSerializer);
        checkValueSize(valueBytes);
        final ResultSet resultSet = session.execute(bindKeyAndValue(putIfAbsentStatement, serialize(key, keySerializer), valueBytes));
        if (resultSet.wasApplied()) {
            return null;
        }

        // Cassandra returns the existing row's columns on a failed lightweight-transaction application, so the
        // current value is already available on this same result set -- no second round trip is needed.
        final Row existingRow = resultSet.one();
        return existingRow == null ? null : valueDeserializer.deserialize(toByteArray(existingRow.getByteBuffer(valueColumn)));
    }

    @Override
    public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
        final ResultSet resultSet = session.execute(bindKey(existsStatement, serialize(key, keySerializer)));
        return resultSet.iterator().hasNext();
    }

    @Override
    public <K, V> V get(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
        final ResultSet resultSet = session.execute(bindKey(fetchStatement, serialize(key, keySerializer)));
        final Row row = resultSet.one();
        return row == null ? null : valueDeserializer.deserialize(toByteArray(row.getByteBuffer(valueColumn)));
    }

    @Override
    public <K> boolean remove(final K key, final Serializer<K> keySerializer) throws IOException {
        final byte[] keyBytes = serialize(key, keySerializer);
        if (strictRemoval) {
            final ResultSet resultSet = session.execute(bindKey(deleteStatement, keyBytes));
            return resultSet.wasApplied();
        }

        final boolean existed = containsKey(key, keySerializer);
        session.execute(bindKey(deleteStatement, keyBytes));
        return existed;
    }

    @Override
    public <K, V> Map<K, V> subMap(final Set<K> keys, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
        if (keys == null) {
            return null;
        }

        final Map<K, CompletionStage<AsyncResultSet>> pendingFetches = new LinkedHashMap<>(keys.size());
        for (final K key : keys) {
            pendingFetches.put(key, session.executeAsync(bindKey(fetchStatement, serialize(key, keySerializer))));
        }

        final Map<K, V> results = new HashMap<>(keys.size());
        for (final Map.Entry<K, CompletionStage<AsyncResultSet>> pendingFetch : pendingFetches.entrySet()) {
            final K key = pendingFetch.getKey();
            try {
                final AsyncResultSet resultSet = pendingFetch.getValue().toCompletableFuture().get();
                final Row row = resultSet.one();
                results.put(key, row == null ? null : valueDeserializer.deserialize(toByteArray(row.getByteBuffer(valueColumn))));
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while awaiting subMap fetch for key " + key, e);
            } catch (final ExecutionException e) {
                final Throwable cause = e.getCause();
                throw new IOException("Failed to fetch value for key " + key, cause != null ? cause : e);
            }
        }

        return results;
    }

    @Override
    public void close() throws IOException {
        // Per-client resources only. The CqlSession this service owns is a shared resource for the lifetime of
        // the controller service and is torn down in @OnDisabled, not here.
    }

    /** Binds a statement whose parameters are (optionally bucket,) key -- used by get/containsKey/remove/subMap. */
    private BoundStatement bindKey(final PreparedStatement statement, final byte[] keyBytes) {
        return bucketed
                ? statement.bind(computeBucket(keyBytes, partitionCount), ByteBuffer.wrap(keyBytes))
                : statement.bind(ByteBuffer.wrap(keyBytes));
    }

    /** Binds a statement whose parameters are (optionally bucket,) key, value -- used by put/putIfAbsent/getAndPutIfAbsent. */
    private BoundStatement bindKeyAndValue(final PreparedStatement statement, final byte[] keyBytes, final byte[] valueBytes) {
        return bucketed
                ? statement.bind(computeBucket(keyBytes, partitionCount), ByteBuffer.wrap(keyBytes), ByteBuffer.wrap(valueBytes))
                : statement.bind(ByteBuffer.wrap(keyBytes), ByteBuffer.wrap(valueBytes));
    }

    private <T> byte[] serialize(final T value, final Serializer<T> serializer) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.serialize(value, out);
        return out.toByteArray();
    }

    private void checkValueSize(final byte[] valueBytes) {
        if (valueBytes.length > maxValueSizeBytes) {
            throw new SerializationException("Serialized value size " + valueBytes.length + " bytes exceeds the configured "
                    + "Maximum Value Size of " + maxValueSizeBytes + " bytes");
        }
    }

    private static byte[] toByteArray(final ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        final ByteBuffer duplicate = buffer.duplicate();
        final byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }
}
