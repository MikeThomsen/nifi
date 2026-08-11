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
package org.apache.nifi.service.cql.api.service;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.service.cql.api.constants.ConnectionCompression;
import org.apache.nifi.service.cql.api.constants.CqlBatchType;
import org.apache.nifi.service.cql.api.constants.CqlConsistencyLevel;
import org.apache.nifi.service.cql.api.constants.UpdateMethod;
import org.apache.nifi.service.cql.api.exception.QueryFailureException;
import org.apache.nifi.service.cql.api.lookup.CqlStatementResult;
import org.apache.nifi.service.cql.api.metadata.PrimaryKey;
import org.apache.nifi.service.cql.api.metadata.PrimaryKeyIdentifier;
import org.apache.nifi.service.cql.api.metadata.QualifiedTableName;
import org.apache.nifi.ssl.SSLContextService;

import java.util.List;
import java.util.Map;

/**
 * Connection abstraction for Apache Cassandra/ScyllaDB, consumed by {@code PutCQLRecord},
 * {@code ExecuteCQLQueryRecord} and {@code CQLDistributedMapCache} without any of them needing to know which
 * backend-specific driver implementation ({@code CassandraCQLExecutionService} or
 * {@code ScyllaDBCQLExecutionService}) is actually configured. Property descriptors here are shared
 * connection settings (contact points, datacenter, credentials, TLS, timeouts, etc.) that both
 * implementations expose identically; the methods below are the query/write contract every implementation
 * must provide.
 */
public interface CQLExecutionService extends ControllerService {
    PropertyDescriptor CONTACT_POINTS = new PropertyDescriptor.Builder()
            .name("Cassandra Contact Points")
            .description("Contact points are addresses of Cassandra nodes, as a comma-separated list of "
                    + "hostname:port entries - for example node1:9042,node2:9042. An IPv6 address must be "
                    + "bracketed to carry a port, as [::1]:9042. If an entry names no port, the default "
                    + "client port of 9042 is used.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(ContactPoints.VALIDATOR)
            .build();

    PropertyDescriptor DATACENTER = new PropertyDescriptor.Builder()
            .name("Cassandra Datacenter")
            .description("The datacenter setting to use with your node/cluster.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor KEYSPACE = new PropertyDescriptor.Builder()
            .name("Default Keyspace")
            .description("The Cassandra Keyspace to connect to. If no keyspace is specified, the query will need to " +
                    "include the keyspace name before any table reference, in case of 'query' native processors or " +
                    "if the processor supports the 'Table' property, the keyspace name has to be provided with the " +
                    "table name in the form of <KEYSPACE>.<TABLE>")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username to access the Cassandra cluster")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password to access the Cassandra cluster")
            .required(false)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor.Builder()
            .name("Consistency Level")
            .description("The strategy for how many replicas must respond before results are returned.")
            .required(true)
            .allowableValues(CqlConsistencyLevel.class)
            .defaultValue(CqlConsistencyLevel.ONE.getValue())
            .build();

    PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("Fetch size")
            .description("The number of result rows to be fetched from the result set at a time - the page size the "
                    + "driver requests per round trip, not a cap on total rows returned. Zero is the default and means "
                    + "the driver's own default page size (5000) is used.")
            .defaultValue("0")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("Compression Type")
            .description("Enable compression at transport-level requests and responses")
            .required(false)
            .allowableValues(ConnectionCompression.class)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(ConnectionCompression.NONE.getValue())
            .build();

    PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Read Timeout")
            .description("Read timeout. 0 means no timeout. If no value is set, the underlying default will be used.")
            .required(false)
            .defaultValue("30 sec")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connect Timeout")
            .description("Connection timeout. 0 means no timeout. If no value is set, the underlying default will be used.")
            .required(false)
            .defaultValue("30 sec")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    PropertyDescriptor DEFAULT_TTL = new PropertyDescriptor.Builder()
            .name("Default Time To Live")
            .description("The default Time To Live (TTL) to apply to INSERT statements and SET-method UPDATE statements when a "
                    + "processor does not override it. Counter UPDATE statements never have a TTL applied, since Cassandra/ScyllaDB do not "
                    + "support one on counter columns. If not set, no TTL is applied and the table's own default_time_to_live (if any) governs.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    /**
     * The precedence described here (file settings override the other connection properties, with
     * fallback to those properties for anything the file omits) mirrors the composition semantics of
     * {@code DriverConfigLoader.compose(primary, fallback)} in the DataStax Java Driver, where "the driver
     * reads an option [from] the 'primary' config ... first[; i]f the option is missing, then it will be
     * looked up in the 'fallback' config" (javadoc on
     * {@code com.datastax.oss.driver.api.core.config.DriverConfigLoader#compose}, java-driver-core
     * 4.19.3, {@code DriverConfigLoader.java}). ScyllaDB's shard-aware driver (com.scylladb:java-driver-core)
     * is a fork that retains the same {@code com.datastax.oss.driver.api.core.config} package and HOCON
     * config format, so the same file and precedence apply unchanged when this service targets ScyllaDB.
     */
    PropertyDescriptor DRIVER_CONFIGURATION_FILE = new PropertyDescriptor.Builder()
            .name("Driver Configuration File")
            .description("Path to an optional Java Driver configuration file using the driver's standard "
                    + "typesafe-config (HOCON) format, for example an application.conf providing advanced "
                    + "settings such as load balancing policy, retry policy, or speculative execution that "
                    + "are not otherwise exposed as properties on this service. Settings in this file take "
                    + "precedence over the other connection properties configured here; any setting not "
                    + "present in the file falls back to those properties or the driver's built-in defaults. "
                    + "ScyllaDB's Java Driver is a shard-aware fork of the DataStax Java Driver that reads the "
                    + "same configuration format, so this file works unchanged when connecting to ScyllaDB.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    /**
     * Executes a CQL statement (typically a {@code SELECT}) and streams results back one row at a time via
     * {@code callback.receive(...)} rather than materializing the whole result set, so arbitrarily large
     * result sets are supported.
     *
     * <p>Values for {@code cql}'s {@code ?} bind markers are supplied via {@code parameters} rather than
     * being concatenated into the statement text, so a value originating outside the flow's configuration -
     * a FlowFile attribute, say - is sent to the server as data and can never be parsed as CQL. Prefer this
     * over interpolating such a value into {@code cql} itself.
     *
     * @param cql the CQL statement text, with {@code ?} bind markers for any positional {@code parameters}
     * @param parameters positional values bound to {@code cql}'s bind markers, in order; may be {@code null}
     * or empty for a statement with no parameters. Implementations are expected to convert each value to the
     * Java type its bind marker's declared CQL type requires, so a value supplied as text still binds to a
     * non-text column
     * @param callback invoked once per result row; the last row is delivered with {@code hasMore = false}
     * @param overrides per-call fetch size/timeout overrides; use {@link QueryOverrides#NONE} for none
     * @throws QueryFailureException if the query failed at the driver/server level (as opposed to, for
     * example, malformed input, which implementations may instead surface as an unchecked exception)
     */
    void query(String cql, List<Object> parameters, CQLQueryCallback callback, QueryOverrides overrides) throws QueryFailureException;

    /**
     * Executes a statement and returns its outcome, including whether a conditional write applied.
     *
     * <p>This is the counterpart to {@link #query}, for two things that method cannot express. The first is
     * the outcome of a conditional write - {@code IF NOT EXISTS}, {@code IF EXISTS}, {@code IF <condition>} -
     * which is a statement-level fact with nowhere to live in a per-row callback, and which a caller
     * implementing compare-and-set has no way to work around: reading before writing is not the same
     * operation and is not safe under concurrency. The second is reading values without a schema over them,
     * for a caller that already owns its encoding and would only have to undo any conversion applied on its
     * behalf.
     *
     * <p>Intended for statements with a bounded result - conditional writes, single-row lookups, existence
     * probes - because the rows are materialized before returning. {@link #query} remains the entry point for
     * result sets large enough to need streaming.
     *
     * @param cql the CQL statement text, with {@code ?} bind markers for any positional {@code parameters}.
     * Not restricted to {@code SELECT}
     * @param parameters positional values bound to {@code cql}'s bind markers, in order; may be {@code null}
     * or empty. Converted to the Java type each bind marker's declared CQL type requires, as in {@link #query}
     * @param overrides per-call fetch size/timeout overrides; use {@link QueryOverrides#NONE} for none
     * @return the statement's outcome; never {@code null}
     * @throws QueryFailureException if the statement failed at the driver/server level
     */
    CqlStatementResult execute(String cql, List<Object> parameters, QueryOverrides overrides) throws QueryFailureException;

    /**
     * Writes a single record to {@code table} via {@code INSERT}. Each record field is written to the column
     * of the same name by default, including primary key columns - Cassandra/ScyllaDB require all primary
     * key columns to be supplied as ordinary column values in an {@code INSERT}, the same as any other
     * column - unless overridden per {@code primaryKeyOverrides}.
     *
     * @param table the keyspace-qualified (or, if the connection has a default keyspace, unqualified) target table
     * @param record the record to write
     * @param primaryKeyOverrides for a given keyspace/table/column identified by a {@link PrimaryKeyIdentifier}
     * key, evaluates the associated {@link RecordPath} against the record to resolve that column's value
     * instead of looking up a same-named record field; the RecordPath must resolve to exactly one value
     * @param overrides per-call TTL/write-timestamp overrides; use {@link WriteOverrides#NONE} for none
     * @throws QueryFailureException if the write failed at the driver/server level
     */
    void insert(QualifiedTableName table, org.apache.nifi.serialization.record.Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides, WriteOverrides overrides) throws QueryFailureException;

    /**
     * Batch form of {@link #insert(QualifiedTableName, org.apache.nifi.serialization.record.Record, Map, WriteOverrides)}:
     * writes every record in {@code records} to {@code table} as a single {@code BatchStatement} of the given
     * {@code batchType}. All records are expected to share the same schema, since one prepared statement is
     * built from the first record and reused for the rest. A {@code null} or empty {@code records} is a no-op.
     *
     * @throws QueryFailureException if the batch failed at the driver/server level
     */
    void insert(QualifiedTableName table, List<org.apache.nifi.serialization.record.Record> records, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides, CqlBatchType batchType, WriteOverrides overrides) throws QueryFailureException;

    /**
     * @return an identifier for where a write to {@code tableName} went, suitable for provenance reporting -
     * not a literal, dereferenceable network URL
     */
    String getTransitUrl(QualifiedTableName tableName);

    /**
     * Deletes the single row identified by {@code updateKeys} from {@code cassandraTable} via {@code DELETE}.
     *
     * @param record supplies the values for each column named in {@code updateKeys}, by matching field name
     * @param primaryKeyOverrides as in {@link #insert}, resolves a specific column's value via a RecordPath
     * instead of a same-named record field
     * @param updateKeys the column names identifying the row to delete; must not be {@code null} or empty,
     * and every name must be present as a field in {@code record}
     * @throws QueryFailureException if the delete failed at the driver/server level
     */
    void delete(QualifiedTableName cassandraTable, org.apache.nifi.serialization.record.Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides, List<String> updateKeys) throws QueryFailureException;

    /**
     * Writes a single record to {@code cassandraTable} via {@code UPDATE}. {@code updateKeys} name the
     * columns that identify the row (the {@code WHERE} clause); every other field in the record is applied
     * according to {@code updateMethod}.
     *
     * @param primaryKeyOverrides as in {@link #insert}, resolves a specific column's value via a RecordPath
     * instead of a same-named record field
     * @param updateKeys the column names identifying the row to update; must not be {@code null} or empty,
     * and every name must be present as a field in {@code record}
     * @param updateMethod {@code SET} to overwrite a column's value, or {@code INCREMENT}/{@code DECREMENT}
     * to adjust a counter column
     * @param overrides per-call TTL/write-timestamp overrides; ignored for {@code INCREMENT}/{@code DECREMENT},
     * since Cassandra/ScyllaDB do not support a TTL or custom write timestamp on counter columns
     * @throws QueryFailureException if the update failed at the driver/server level
     */
    void update(QualifiedTableName cassandraTable, org.apache.nifi.serialization.record.Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides, List<String> updateKeys, UpdateMethod updateMethod, WriteOverrides overrides) throws QueryFailureException;

    /**
     * Batch form of {@link #update(QualifiedTableName, org.apache.nifi.serialization.record.Record, Map, List, UpdateMethod, WriteOverrides)}:
     * applies the same update to every record in {@code records} against {@code cassandraTable} as a single
     * {@code BatchStatement} of the given {@code batchType}. All records are expected to share the same
     * schema, since one prepared statement is built from the first record and reused for the rest.
     * {@code INCREMENT}/{@code DECREMENT} requires {@code batchType} to be {@code COUNTER} or
     * {@code UNLOGGED}, since Cassandra/ScyllaDB reject counter mutations in any other batch type. A
     * {@code null} or empty {@code records} is a no-op.
     *
     * @throws QueryFailureException if the batch failed at the driver/server level
     */
    void update(QualifiedTableName cassandraTable, List<org.apache.nifi.serialization.record.Record> records, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides, List<String> updateKeys, UpdateMethod updateMethod,
                CqlBatchType batchType, WriteOverrides overrides) throws QueryFailureException;

    /**
     * @param table the keyspace-qualified (or, if the connection has a default keyspace, unqualified) table
     * to describe
     * @return {@code table}'s primary key structure - its partition key columns and clustering columns, each
     * in their real declared order - resolved from the cluster's own schema metadata
     */
    PrimaryKey getMetadata(QualifiedTableName table);
}
