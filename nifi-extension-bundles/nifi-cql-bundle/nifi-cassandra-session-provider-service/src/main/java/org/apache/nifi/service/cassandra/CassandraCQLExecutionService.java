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
package org.apache.nifi.service.cassandra;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.service.cassandra.mapping.CassandraUdtSchemaMapper;
import org.apache.nifi.service.cassandra.mapping.CharacterCodec;
import org.apache.nifi.service.cassandra.mapping.FlexibleBigIntCodec;
import org.apache.nifi.service.cassandra.mapping.FlexibleBooleanCodec;
import org.apache.nifi.service.cassandra.mapping.FlexibleCounterCodec;
import org.apache.nifi.service.cassandra.mapping.FlexibleDateCodec;
import org.apache.nifi.service.cassandra.mapping.FlexibleDoubleCodec;
import org.apache.nifi.service.cassandra.mapping.FlexibleFloatCodec;
import org.apache.nifi.service.cassandra.mapping.FlexibleIntCodec;
import org.apache.nifi.service.cassandra.mapping.FlexibleSmallIntCodec;
import org.apache.nifi.service.cassandra.mapping.FlexibleTimeCodec;
import org.apache.nifi.service.cassandra.mapping.FlexibleTinyIntCodec;
import org.apache.nifi.service.cassandra.mapping.JavaSQLDateCodec;
import org.apache.nifi.service.cassandra.mapping.JavaSQLTimeCodec;
import org.apache.nifi.service.cassandra.mapping.JavaSQLTimestampCodec;
import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.service.cql.api.service.CQLQueryCallback;
import org.apache.nifi.service.cql.api.service.ContactPoints;
import org.apache.nifi.service.cql.api.lookup.CqlCell;
import org.apache.nifi.service.cql.api.lookup.CqlRow;
import org.apache.nifi.service.cql.api.lookup.CqlStatementResult;
import org.apache.nifi.service.cql.api.lookup.impl.StandardCqlCell;
import org.apache.nifi.service.cql.api.lookup.impl.StandardCqlRow;
import org.apache.nifi.service.cql.api.lookup.impl.StandardCqlStatementResult;
import org.apache.nifi.service.cql.api.constants.CqlBatchType;
import org.apache.nifi.service.cql.api.service.QueryOverrides;
import org.apache.nifi.service.cql.api.constants.UpdateMethod;
import org.apache.nifi.service.cql.api.service.WriteOverrides;
import org.apache.nifi.service.cql.api.exception.QueryFailureException;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.service.cql.api.metadata.PrimaryKey;
import org.apache.nifi.service.cql.api.constants.PrimaryKeyFieldType;
import org.apache.nifi.service.cql.api.metadata.PrimaryKeyIdentifier;
import org.apache.nifi.service.cql.api.metadata.PrimaryKeyMetadata;
import org.apache.nifi.service.cql.api.metadata.QualifiedTableName;
import org.apache.nifi.ssl.SSLContextService;

import java.io.File;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.net.ssl.SSLContext;

@Tags({"cassandra", "cql", "database", "connection", "session", "pooling"})
@CapabilityDescription("Provides a CQL connection session for CQL processors and controller services to work with Apache Cassandra. "
        + "Use this service for Apache Cassandra clusters; use ScyllaDBCQLExecutionService for ScyllaDB.")
// Referenced by name rather than by class: a session provider has no business depending on the modules that consume
// it, and nifi-cql-processors in particular could not accept the dependency - the driver this module carries is what
// its ban-database-client-dependencies enforcer rule exists to keep out.
@SeeAlso(classNames = {
        "org.apache.nifi.processors.cql.PutCQLRecord",
        "org.apache.nifi.processors.cql.ExecuteCQLQueryRecord",
        "org.apache.nifi.service.cql.cache.CQLDistributedMapCache",
        "org.apache.nifi.service.scylladb.ScyllaDBCQLExecutionService"
})
public class CassandraCQLExecutionService extends AbstractControllerService implements CQLExecutionService, VerifiableControllerService {

    public static final int DEFAULT_CASSANDRA_PORT = ContactPoints.DEFAULT_PORT;

    /**
     * The column a conditional statement's outcome arrives in. Named with brackets by the protocol precisely
     * because no CQL identifier may contain them, so it can never collide with a real column.
     */
    private static final String APPLIED_COLUMN = "[applied]";

    /**
     * Bind marker name for an explicit per-record write timestamp (see {@link WriteOverrides#timestampField()}).
     * Namespaced to make an accidental collision with a real column name effectively impossible.
     */
    private static final String WRITE_TIMESTAMP_BIND_MARKER = "nifi_write_timestamp";

    private CqlSession cassandraSession;

    private int pageSize;

    private Duration defaultTtl;

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CONTACT_POINTS,
            DATACENTER,
            KEYSPACE,
            USERNAME,
            PASSWORD,
            PROP_SSL_CONTEXT_SERVICE,
            FETCH_SIZE,
            READ_TIMEOUT,
            CONNECT_TIMEOUT,
            CONSISTENCY_LEVEL,
            COMPRESSION_TYPE,
            DEFAULT_TTL,
            DRIVER_CONFIGURATION_FILE
    );

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        tableMetadataCache = new ConcurrentHashMap<>();
        connectToCassandra(context);
    }

    @OnDisabled
    public void onDisabled() {
        if (cassandraSession != null) {
            cassandraSession.close();
            cassandraSession = null;
        }
    }

    private Map<QualifiedTableName, PrimaryKey> tableMetadataCache;
    private String defaultKeyspace;

    /**
     * Fills in the connection's default keyspace for an unqualified table name, mirroring what the driver
     * session itself already does implicitly for query-builder statements built without an explicit
     * keyspace. Metadata lookups have no such implicit fallback - {@code Metadata.getKeyspace(...)} needs a
     * real keyspace name - so this makes that resolution explicit wherever a {@link QualifiedTableName} is
     * used to look up table metadata rather than just to build a statement.
     */
    private QualifiedTableName resolveKeyspace(QualifiedTableName table) {
        return table.isQualified() ? table : new QualifiedTableName(defaultKeyspace, table.table());
    }

    private void connectToCassandra(ConfigurationContext context) {
        if (cassandraSession == null) {
            this.pageSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions().asInteger();

            final PropertyValue defaultTtlProperty = context.getProperty(DEFAULT_TTL).evaluateAttributeExpressions();
            this.defaultTtl = defaultTtlProperty.isSet() ? defaultTtlProperty.asDuration() : null;

            final CqlSession cqlSession = buildSession(context);

            try {
                MutableCodecRegistry codecRegistry =
                        (MutableCodecRegistry) cqlSession.getContext().getCodecRegistry();

                codecRegistry.register(new JavaSQLTimestampCodec());
                codecRegistry.register(new JavaSQLDateCodec());
                codecRegistry.register(new JavaSQLTimeCodec());
                codecRegistry.register(new CharacterCodec());
                codecRegistry.register(new FlexibleCounterCodec());
                codecRegistry.register(new FlexibleBooleanCodec());
                codecRegistry.register(new FlexibleTinyIntCodec());
                codecRegistry.register(new FlexibleSmallIntCodec());
                codecRegistry.register(new FlexibleIntCodec());
                codecRegistry.register(new FlexibleBigIntCodec());
                codecRegistry.register(new FlexibleFloatCodec());
                codecRegistry.register(new FlexibleDoubleCodec());
                codecRegistry.register(new FlexibleDateCodec());
                codecRegistry.register(new FlexibleTimeCodec());
            } catch (Exception ex) {
                cqlSession.close();
                throw new ProcessException("There was an error registering conversion codecs.", ex);
            }

            cassandraSession = cqlSession;
        }
    }

    private CqlSession buildSession(final ConfigurationContext context) {
        final String consistencyLevel = context.getProperty(CONSISTENCY_LEVEL).getValue();
        final String compression = context.getProperty(COMPRESSION_TYPE).getValue();
        final String contactPointList = context.getProperty(CONTACT_POINTS).evaluateAttributeExpressions().getValue();

        List<InetSocketAddress> contactPoints = getContactPoints(contactPointList);

        // Set up the client for secure (SSL/TLS communications) if configured to do so
        final SSLContextService sslService =
                context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final SSLContext sslContext;

        if (sslService == null) {
            sslContext = null;
        } else {
            sslContext = sslService.createContext();
        }

        final String username;
        final String password;
        PropertyValue usernameProperty = context.getProperty(USERNAME).evaluateAttributeExpressions();
        PropertyValue passwordProperty = context.getProperty(PASSWORD).evaluateAttributeExpressions();

        if (usernameProperty != null && passwordProperty != null) {
            username = usernameProperty.getValue();
            password = passwordProperty.getValue();
        } else {
            username = null;
            password = null;
        }

        final Duration readTimeout = context.getProperty(READ_TIMEOUT).evaluateAttributeExpressions().asDuration();
        final Duration connectTimeout = context.getProperty(CONNECT_TIMEOUT).evaluateAttributeExpressions().asDuration();

        final String datacenter = context.getProperty(DATACENTER).evaluateAttributeExpressions().getValue();

        final String sessionKeyspace = context.getProperty(KEYSPACE).evaluateAttributeExpressions().getValue();
        this.defaultKeyspace = sessionKeyspace;

        final DriverConfigLoader propertyBasedLoader =
                DriverConfigLoader.programmaticBuilder()
                        .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, connectTimeout)
                        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, readTimeout)
                        .withString(DefaultDriverOption.REQUEST_CONSISTENCY, consistencyLevel)
                        .withString(DefaultDriverOption.PROTOCOL_COMPRESSION, compression)
                        .build();

        final DriverConfigLoader loader = buildConfigLoader(context, propertyBasedLoader);

        CqlSessionBuilder builder = CqlSession.builder()
                .addContactPoints(contactPoints);

        if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
            builder = builder.withAuthCredentials(username, password);
        }

        return builder
                .withSslContext(sslContext)
                .withLocalDatacenter(datacenter)
                .withKeyspace(sessionKeyspace)
                .withConfigLoader(loader)
                .build();
    }

    /**
     * Builds the {@link DriverConfigLoader} used to open the session. When a Driver Configuration File is
     * configured, it is composed as the primary source ahead of {@code propertyBasedLoader}, so any option
     * defined in the file overrides the corresponding connection property, while options absent from the
     * file still fall back to the property-derived configuration. The file uses the driver's standard
     * typesafe-config format, which the ScyllaDB Java Driver reads identically, so this method requires no
     * override for ScyllaDB compatibility.
     */
    protected DriverConfigLoader buildConfigLoader(final ConfigurationContext context, final DriverConfigLoader propertyBasedLoader) {
        final PropertyValue configFileProperty = context.getProperty(DRIVER_CONFIGURATION_FILE);

        if (configFileProperty == null || !configFileProperty.isSet()) {
            return propertyBasedLoader;
        }

        final String configFilePath = configFileProperty.evaluateAttributeExpressions().getValue();
        final DriverConfigLoader fileBasedLoader = DriverConfigLoader.fromFile(new File(configFilePath));

        return DriverConfigLoader.compose(fileBasedLoader, propertyBasedLoader);
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger, final Map<String, String> variables) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        final String datacenter = context.getProperty(DATACENTER).evaluateAttributeExpressions().getValue();

        CqlSession session = null;
        try {
            session = buildSession(context);
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

        if (session != null) {
            try {
                // CqlSessionBuilder.build() succeeds even when no node belongs to the configured local
                // datacenter: the driver only filters nodes by datacenter when computing a query plan for
                // an actual request. Executing a trivial statement here forces that node selection so a
                // bad datacenter name is caught during verification instead of on first real query.
                try {
                    session.execute("SELECT release_version FROM system.local");
                    results.add(new ConfigVerificationResult.Builder()
                            .verificationStepName("Verify Datacenter")
                            .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                            .explanation("Found an available node in datacenter [" + datacenter + "]")
                            .build());
                } catch (final Exception e) {
                    verificationLogger.warn("No available node found in datacenter [{}]: {}", datacenter, e.getMessage());
                    results.add(new ConfigVerificationResult.Builder()
                            .verificationStepName("Verify Datacenter")
                            .outcome(ConfigVerificationResult.Outcome.FAILED)
                            .explanation("No available node found in datacenter [" + datacenter + "]: " + e.getMessage())
                            .build());
                }

                final String keyspaceName = context.getProperty(KEYSPACE).isSet()
                        ? context.getProperty(KEYSPACE).evaluateAttributeExpressions().getValue() : null;

                if (StringUtils.isNotBlank(keyspaceName)) {
                    final boolean keyspaceFound = session.getMetadata().getKeyspace(keyspaceName).isPresent();
                    results.add(new ConfigVerificationResult.Builder()
                            .verificationStepName("Verify Keyspace")
                            .outcome(keyspaceFound ? ConfigVerificationResult.Outcome.SUCCESSFUL : ConfigVerificationResult.Outcome.FAILED)
                            .explanation(keyspaceFound
                                    ? "Found keyspace [" + keyspaceName + "]"
                                    : "Keyspace [" + keyspaceName + "] was not found in the cluster")
                            .build());
                }
            } finally {
                session.close();
            }
        }

        return results;
    }

    /**
     * Parsing is delegated to {@link ContactPoints}, the same rules the property's validator applies, so a
     * value that validated cannot fail here and an Expression-Language-produced value that skipped validation
     * fails with the same messages it would have gotten from the validator. Name resolution happens at this
     * point, not during parsing.
     */
    private List<InetSocketAddress> getContactPoints(String contactPointList) {
        return ContactPoints.parse(contactPointList).stream()
                .map(contactPoint -> new InetSocketAddress(contactPoint.host(), contactPoint.port()))
                .collect(Collectors.toList());
    }

    private void cacheMetadata(QualifiedTableName table) {
        final QualifiedTableName resolved = resolveKeyspace(table);
        getLogger().debug("Fetching metadata for {}.{}", resolved.keyspace(), resolved.table());

        if (!tableMetadataCache.containsKey(resolved)) {
            getLogger().debug("Metadata for {}.{} not found, fetching from cluster.", resolved.keyspace(), resolved.table());
            Optional<KeyspaceMetadata> keyspaceOpt = cassandraSession.getMetadata()
                    .getKeyspace(resolved.keyspace());
            if (keyspaceOpt.isEmpty()) {
                throw new RuntimeException("Empty keyspace metadata");
            }

            KeyspaceMetadata keyspaceMetadata = keyspaceOpt.get();
            Optional<TableMetadata> tableOpt = keyspaceMetadata.getTable(resolved.table());
            if (tableOpt.isEmpty()) {
                throw new RuntimeException("Empty table metadata");
            }

            TableMetadata metadata = tableOpt.get();
            PrimaryKey tableInfo = convertTableMetadata(metadata);

            tableMetadataCache.put(resolved, tableInfo);
            getLogger().debug("Metadata fetched and cached for {}.{}", resolved.keyspace(), resolved.table());
        }
    }

    private PrimaryKey convertTableMetadata(TableMetadata metadata) {
        List<PrimaryKeyMetadata> partitions = new ArrayList<>();
        List<PrimaryKeyMetadata> clustering = new ArrayList<>();

        int loc = 0;
        for (ColumnMetadata m : metadata.getPartitionKey()) {
            partitions.add(new PrimaryKeyMetadata(m.getName().asInternal(), loc++, PrimaryKeyFieldType.PARTITION));
        }

        loc = 0;
        for (ColumnMetadata m : metadata.getClusteringColumns().keySet()) {
            clustering.add(new PrimaryKeyMetadata(m.getName().asInternal(), loc++, PrimaryKeyFieldType.CLUSTERING));
        }

        return new PrimaryKey(partitions, clustering);
    }

    @Override
    public void query(String cql, List<Object> parameters, CQLQueryCallback callback, QueryOverrides overrides) throws QueryFailureException {
        BoundStatement boundStatement = bindStatement(cql, parameters, overrides);

        AtomicReference<RecordSchema> schemaReference = new AtomicReference<>();

        // The whole lifecycle of the query - the initial execute() as well as every page fetch triggered by
        // hasNext()/next() as the ResultSet is iterated - can surface a QueryExecutionException, so all of it
        // is covered by one try, rather than only the page-fetch calls.
        try {
            ResultSet results = cassandraSession.execute(boundStatement);
            Iterator<Row> resultsIterator = results.iterator();
            long rowNumber = 0;

            List<String> columnNames = new ArrayList<>();

            while (resultsIterator.hasNext()) {
                Row row = resultsIterator.next();

                if (schemaReference.get() == null) {
                    Schema generatedAvroSchema = createSchema(results);
                    RecordSchema converted = AvroTypeUtil.createSchema(generatedAvroSchema);
                    schemaReference.set(converted);
                }

                if (columnNames.isEmpty()) {
                    row.getColumnDefinitions().forEach(def -> {
                        columnNames.add(def.getName().toString());
                    });
                }

                Map<String, Object> resultMap = new HashMap<>();

                for (int x = 0; x < columnNames.size(); x++) {
                    resultMap.put(columnNames.get(x), row.getObject(x));
                }

                MapRecord record = new MapRecord(schemaReference.get(), resultMap);

                callback.receive(++rowNumber, record, resultsIterator.hasNext());
            }
        // Each of these is worth another attempt: a coordinator/replica-level execution failure, no reachable
        // node, or a client-side request timeout. Deliberately not their DriverException base type - the
        // validation errors under it (SyntaxError, InvalidQueryException, UnauthorizedException) can never
        // succeed on a retry, so they belong on the failure path below.
        } catch (QueryExecutionException | AllNodesFailedException | DriverTimeoutException qee) {
            getLogger().error("Error executing query", qee);
            callback.clear();
            throw new QueryFailureException();
        } catch (Exception ex) {
            callback.clear();
            throw new ProcessException("Error querying CQL", ex);
        }
    }

    @Override
    public CqlStatementResult execute(final String cql, final List<Object> parameters, final QueryOverrides overrides)
            throws QueryFailureException {
        final BoundStatement boundStatement = bindStatement(cql, parameters, overrides);

        try {
            final ResultSet results = cassandraSession.execute(boundStatement);
            final boolean wasApplied = results.wasApplied();

            // The outcome column is present exactly when the statement was conditional - wasApplied() alone
            // cannot tell us, since it also returns true for every unconditional statement.
            final boolean conditional = results.getColumnDefinitions().contains(APPLIED_COLUMN);

            // A conditional write that applied has no prior value to report, so it reports no rows. The
            // backends disagree about what they send in that case: Cassandra returns the outcome column
            // alone, ScyllaDB returns the other columns alongside it. Normalizing here is the whole point of
            // having one API over both - otherwise callers would find rows().isEmpty() backend-dependent.
            if (conditional && wasApplied) {
                return new StandardCqlStatementResult(true, List.of());
            }

            final List<CqlRow> rows = new ArrayList<>();
            for (final Row row : results) {
                rows.add(new StandardCqlRow(toCells(row)));
            }

            return new StandardCqlStatementResult(wasApplied, rows);
        } catch (QueryExecutionException | AllNodesFailedException | DriverTimeoutException qee) {
            getLogger().error("Error executing statement", qee);
            throw new QueryFailureException();
        } catch (Exception ex) {
            throw new ProcessException("Error executing CQL", ex);
        }
    }

    /**
     * Converts one driver row into cells, dropping the conditional-outcome column. That column is reported
     * through {@link CqlStatementResult#wasApplied()} instead: it is a statement-level fact, and its name -
     * {@code [applied]} - is not a legal identifier in CQL or in a NiFi record schema, so representing it as
     * data would push that problem onto every caller.
     */
    private List<CqlCell> toCells(final Row row) {
        final ColumnDefinitions definitions = row.getColumnDefinitions();
        final List<CqlCell> cells = new ArrayList<>(definitions.size());

        for (int index = 0; index < definitions.size(); index++) {
            final String columnName = definitions.get(index).getName().toString();
            if (APPLIED_COLUMN.equals(columnName)) {
                continue;
            }

            final DataType columnType = definitions.get(index).getType();
            cells.add(hasJdkRepresentation(columnType)
                    ? new StandardCqlCell(columnName, row.getObject(index))
                    : StandardCqlCell.unsupported(columnName, columnType.asCql(true, false)));
        }

        return cells;
    }

    /**
     * Whether a CQL type decodes to something from the JDK rather than to a driver class. User defined types,
     * tuples and {@code duration} decode to {@code UdtValue}, {@code TupleValue} and {@code CqlDuration}
     * respectively, and handing one of those to a caller would put a driver class on the far side of an API
     * whose whole purpose is that no driver class crosses it. Collections are checked through to their
     * elements, since a {@code list<some_udt>} decodes to a JDK List of driver objects.
     */
    private static boolean hasJdkRepresentation(final DataType type) {
        return switch (type) {
            case UserDefinedType ignored -> false;
            case TupleType ignored -> false;
            case ListType list -> hasJdkRepresentation(list.getElementType());
            case SetType set -> hasJdkRepresentation(set.getElementType());
            case MapType map -> hasJdkRepresentation(map.getKeyType()) && hasJdkRepresentation(map.getValueType());
            default -> !DataTypes.DURATION.equals(type);
        };
    }

    /**
     * Builds and binds a statement, applying any per-call fetch size and timeout overrides. Shared by
     * {@link #query} and {@link #execute}, which differ only in what they do with the result.
     */
    private BoundStatement bindStatement(final String cql, final List<Object> parameters, final QueryOverrides overrides) {
        SimpleStatementBuilder statementBuilder = SimpleStatement.builder(cql)
                .setPageSize(resolveFetchSize(overrides, pageSize));

        final Duration timeoutOverride = resolveTimeoutOverride(overrides);
        if (timeoutOverride != null) {
            statementBuilder = statementBuilder.setTimeout(timeoutOverride);
        }

        final SimpleStatement statement = statementBuilder.build();
        final PreparedStatement preparedStatement = cassandraSession.prepare(statement);

        return parameters != null && !parameters.isEmpty()
                ? preparedStatement.bind(toBindValues(preparedStatement, parameters))
                : preparedStatement.bind();
    }

    static int resolveFetchSize(final QueryOverrides overrides, final int defaultFetchSize) {
        return overrides != null && overrides.fetchSize() != null ? overrides.fetchSize() : defaultFetchSize;
    }

    static Duration resolveTimeoutOverride(final QueryOverrides overrides) {
        return overrides != null ? overrides.timeout() : null;
    }

    static BatchType toDriverBatchType(final CqlBatchType batchType) {
        return switch (batchType) {
            case LOGGED -> BatchType.LOGGED;
            case UNLOGGED -> BatchType.UNLOGGED;
            case COUNTER -> BatchType.COUNTER;
        };
    }

    /**
     * @return the TTL to apply in seconds, or {@code null} if none applies. {@code Integer} (not {@code int}) so
     * "no TTL" can be represented distinctly from "a TTL of zero seconds" (which Cassandra treats as an explicit
     * request to clear any table-level default TTL).
     */
    static Integer resolveTtlSeconds(final WriteOverrides overrides, final Duration defaultTtl) {
        final Duration ttl = overrides != null && overrides.ttl() != null ? overrides.ttl() : defaultTtl;
        return ttl != null ? Math.toIntExact(ttl.toSeconds()) : null;
    }

    /**
     * There's no connection-service-level default for this one (unlike TTL): which field holds a record's own
     * event time is a property of the schema being written, not the connection, so it only ever comes from a
     * per-call override.
     */
    static boolean hasTimestampOverride(final WriteOverrides overrides) {
        return overrides != null && overrides.timestampField() != null;
    }

    /**
     * Converts a record field's value into epoch microseconds, the unit Cassandra/ScyllaDB expect for an
     * explicit write timestamp (distinct from the millisecond precision of the CQL {@code timestamp} type).
     */
    static long toEpochMicros(final Object value) {
        final Instant instant;
        if (value instanceof Instant instantValue) {
            instant = instantValue;
        } else if (value instanceof java.util.Date dateValue) {
            instant = dateValue.toInstant();
        } else if (value instanceof Number numberValue) {
            instant = Instant.ofEpochMilli(numberValue.longValue());
        } else {
            throw new IllegalArgumentException("Cannot derive a write timestamp from a value of type "
                    + (value == null ? "null" : value.getClass().getName()));
        }

        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
    }

    protected GeneratedResult generateInsert(QualifiedTableName table, RecordSchema schema,
                                              Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                                              Integer ttlSeconds, boolean includeTimestampMarker) {
        InsertInto insertQuery = table.isQualified()
                ? QueryBuilder.insertInto(table.keyspace(), table.table())
                : QueryBuilder.insertInto(table.table());
        List<String> keys = new ArrayList<>();

        RegularInsert regularInsert = null;
        for (String fieldName : columnNamesIncludingOverrideOnly(table, schema, primaryKeyOverrides)) {
            if (regularInsert == null) {
                regularInsert = insertQuery.value(fieldName, QueryBuilder.bindMarker(fieldName));
            } else {
                regularInsert = regularInsert.value(fieldName, QueryBuilder.bindMarker(fieldName));
            }

            keys.add(fieldName);
        }

        if (regularInsert == null) {
            throw new ProcessException("Could not build an insert statement from the supplied record");
        }

        Insert insert = regularInsert;
        if (ttlSeconds != null) {
            insert = insert.usingTtl(ttlSeconds);
        }
        if (includeTimestampMarker) {
            // A separate named bind marker (set by name on the bound statement, not part of "keys") rather than
            // a literal, since - unlike TTL - each record in a batch can carry a different write timestamp.
            insert = insert.usingTimestamp(QueryBuilder.bindMarker(WRITE_TIMESTAMP_BIND_MARKER));
        }

        return new GeneratedResult(insert.build(), keys);
    }

    /**
     * Every schema field, in schema order, followed by any primary key override target column for this table
     * that has no same-named schema field. Without this, a column whose value is only ever supplied by a
     * primary key override - never by a record field of the same name, such as a {@code date} column derived
     * from a {@code timestamp} field via a {@code format()} RecordPath override - would never get a bind
     * marker in the generated statement at all, and the write would fail (or silently omit that column)
     * regardless of what the override itself evaluates to.
     */
    private List<String> columnNamesIncludingOverrideOnly(QualifiedTableName table, RecordSchema schema,
                                                            Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides) {
        final List<String> columnNames = new ArrayList<>(schema.getFieldNames());
        for (String overrideFieldName : overrideFieldNamesForTable(table, primaryKeyOverrides)) {
            if (!columnNames.contains(overrideFieldName)) {
                columnNames.add(overrideFieldName);
            }
        }
        return columnNames;
    }

    /**
     * The field names of every primary key override configured for this specific table, resolving an
     * unqualified {@code table} against {@link #defaultKeyspace} the same way {@link #getRecordPathOverride}
     * does.
     */
    private Set<String> overrideFieldNamesForTable(QualifiedTableName table, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides) {
        final QualifiedTableName resolved = resolveKeyspace(table);
        return primaryKeyOverrides.keySet().stream()
                .filter(identifier -> identifier.keyspace().equals(resolved.keyspace()) && identifier.tableName().equals(resolved.table()))
                .map(PrimaryKeyIdentifier::fieldName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    public void insert(QualifiedTableName table, org.apache.nifi.serialization.record.Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                        WriteOverrides overrides) throws QueryFailureException {
        final boolean useTimestampOverride = hasTimestampOverride(overrides);
        GeneratedResult result = generateInsert(table, record.getSchema(), primaryKeyOverrides, resolveTtlSeconds(overrides, defaultTtl), useTimestampOverride);
        PreparedStatement preparedStatement = cassandraSession.prepare(result.statement);

        Object[] values = getBindValues(table, primaryKeyOverrides, record, preparedStatement, result.keysUsed);
        if (useTimestampOverride) {
            setTimestampBindValue(values, preparedStatement, record, overrides);
        }
        BoundStatement boundStatement = preparedStatement.bind(values);

        try {
            cassandraSession.execute(boundStatement);
        } catch (QueryExecutionException | AllNodesFailedException | DriverTimeoutException qee) {
            getLogger().error("Error executing insert", qee);
            throw new QueryFailureException();
        }
    }

    @Override
    public void insert(QualifiedTableName table, List<org.apache.nifi.serialization.record.Record> records, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                        CqlBatchType batchType, WriteOverrides overrides) throws QueryFailureException {
        if (records == null || records.isEmpty()) {
            return;
        }

        final boolean useTimestampOverride = hasTimestampOverride(overrides);
        BatchStatementBuilder builder = BatchStatement.builder(toDriverBatchType(batchType));
        GeneratedResult result = generateInsert(table, records.get(0).getSchema(), primaryKeyOverrides, resolveTtlSeconds(overrides, defaultTtl), useTimestampOverride);
        PreparedStatement preparedStatement = cassandraSession.prepare(result.statement);

        for (org.apache.nifi.serialization.record.Record record : records) {
            Object[] values = getBindValues(table, primaryKeyOverrides, record, preparedStatement, result.keysUsed);
            if (useTimestampOverride) {
                setTimestampBindValue(values, preparedStatement, record, overrides);
            }
            builder.addStatement(preparedStatement.bind(values));
        }

        try {
            cassandraSession.execute(builder.build());
        } catch (QueryExecutionException | AllNodesFailedException | DriverTimeoutException qee) {
            getLogger().error("Error executing batch insert", qee);
            throw new QueryFailureException();
        }
    }

    @Override
    public String getTransitUrl(QualifiedTableName tableName) {
        final String qualifiedName = tableName.isQualified()
                ? tableName.keyspace() + "." + tableName.table()
                : tableName.table();
        return  "cassandra://" + cassandraSession.getMetadata().getClusterName() + "." + qualifiedName;
    }

    /**
     * Creates an Avro schema from the given result set. The metadata (column definitions, data types, etc.) is used
     * to determine a schema for Avro.
     *
     * @param rs The result set from which an Avro schema will be created
     * @return An Avro schema corresponding to the given result set's metadata
     */
    public static Schema createSchema(final ResultSet rs) {
        final ColumnDefinitions columnDefinitions = rs.getColumnDefinitions();
        final int nrOfColumns = (columnDefinitions == null ? 0 : columnDefinitions.size());
        String tableName = "NiFi_Cassandra_Query_Record";
        if (nrOfColumns > 0) {
            String tableNameFromMeta = columnDefinitions.get(0).getTable().toString();
            if (!StringUtils.isBlank(tableNameFromMeta)) {
                tableName = tableNameFromMeta;
            }
        }

        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record(tableName).namespace("any.data").fields();
        if (columnDefinitions != null) {
            final Map<String, Schema> udtSchemaCache = new HashMap<>();
            for (int i = 0; i < nrOfColumns; i++) {
                DataType dataType = columnDefinitions.get(i).getType();
                if (dataType == null) {
                    throw new IllegalArgumentException("No data type for column[" + i + "] with name "
                            + columnDefinitions.get(i).getName());
                }

                builder.name(columnDefinitions.get(i).getName().toString())
                        .type(CassandraUdtSchemaMapper.toAvroSchema(dataType, udtSchemaCache))
                        .noDefault();
            }
        }
        return builder.endRecord();
    }

    protected GeneratedResult generateDelete(QualifiedTableName cassandraTable, org.apache.nifi.serialization.record.Record record,
                                             Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides, List<String> deleteKeyNames) {
        RecordSchema schema = record.getSchema();

        if (deleteKeyNames == null || deleteKeyNames.isEmpty()) {
            throw new IllegalArgumentException("No delete keys were specified");
        }

        // Verify every delete key is either a real record field or covered by a primary key override for this
        // table, matching generateUpdate: a key resolved purely by RecordPath has no same-named record field,
        // but getBindValues can still resolve its value from the override alone.
        final Set<String> overrideFieldNames = overrideFieldNamesForTable(cassandraTable, primaryKeyOverrides);
        for (String deleteKey : deleteKeyNames) {
            if (!schema.getFieldNames().contains(deleteKey) && !overrideFieldNames.contains(deleteKey)) {
                throw new IllegalArgumentException("Delete key '" + deleteKey + "' is not present in the record schema");
            }
        }

        DeleteSelection deleteSelection = cassandraTable.isQualified()
                ? QueryBuilder.deleteFrom(cassandraTable.keyspace(), cassandraTable.table())
                : QueryBuilder.deleteFrom(cassandraTable.table());

        List<Relation> whereCriteria = new ArrayList<>();
        List<String> keysUsedInOrder = new ArrayList<>();

        for (String fieldName : deleteKeyNames) {
            whereCriteria.add(Relation.column(fieldName).isEqualTo(QueryBuilder.bindMarker(fieldName)));
            keysUsedInOrder.add(fieldName);
        }

        return new GeneratedResult(deleteSelection.where(whereCriteria).build(), keysUsedInOrder);
    }

    protected GeneratedResult generateUpdate(QualifiedTableName cassandraTable, org.apache.nifi.serialization.record.Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                                              List<String> updateKeyNames, UpdateMethod updateMethod, Integer ttlSeconds, boolean includeTimestampMarker) {
        RecordSchema schema = record.getSchema();

        List<String> keysUsedInOrder = new ArrayList<>();

        if (updateKeyNames == null || updateKeyNames.isEmpty()) {
            throw new IllegalArgumentException("No Update Keys were specified");
        }

        // Verify every update key is either a real record field or covered by a primary key override for
        // this table - an update key with neither, such as a date column derived from a timestamp field via
        // a format() RecordPath override, would otherwise be rejected here even though getBindValues can
        // resolve its value perfectly well from the override alone.
        final Set<String> overrideFieldNames = overrideFieldNamesForTable(cassandraTable, primaryKeyOverrides);
        for (String updateKey : updateKeyNames) {
            if (!schema.getFieldNames().contains(updateKey) && !overrideFieldNames.contains(updateKey)) {
                throw new IllegalArgumentException("Update key '" + updateKey + "' is not present in the record schema");
            }
        }

        UpdateStart updateQueryStart = cassandraTable.isQualified()
                ? QueryBuilder.update(cassandraTable.keyspace(), cassandraTable.table())
                : QueryBuilder.update(cassandraTable.table());

        // Cassandra/ScyllaDB do not support a TTL or a custom write timestamp on counter columns, so both are
        // only applied for SET updates.
        if (ttlSeconds != null && updateMethod == UpdateMethod.SET) {
            updateQueryStart = updateQueryStart.usingTtl(ttlSeconds);
        }
        if (includeTimestampMarker && updateMethod == UpdateMethod.SET) {
            updateQueryStart = updateQueryStart.usingTimestamp(QueryBuilder.bindMarker(WRITE_TIMESTAMP_BIND_MARKER));
        }

        UpdateWithAssignments updateAssignments = null;

        List<String> otherKeys = schema.getFieldNames().stream()
                .filter(fieldName -> !updateKeyNames.contains(fieldName))
                .toList();

        for (String fieldName : otherKeys) {
            if (updateMethod == UpdateMethod.SET) {
                updateAssignments = updateAssignments == null ? updateQueryStart.setColumn(fieldName, QueryBuilder.bindMarker(fieldName))
                        : updateAssignments.setColumn(fieldName, QueryBuilder.bindMarker(fieldName));
            } else if (updateMethod == UpdateMethod.INCREMENT) {
                updateAssignments = updateAssignments == null ? updateQueryStart.increment(fieldName, QueryBuilder.bindMarker(fieldName))
                        : updateAssignments.increment(fieldName, QueryBuilder.bindMarker(fieldName));
            } else if (updateMethod == UpdateMethod.DECREMENT) {
                updateAssignments = updateAssignments == null ? updateQueryStart.decrement(fieldName, QueryBuilder.bindMarker(fieldName))
                        : updateAssignments.decrement(fieldName, QueryBuilder.bindMarker(fieldName));
            } else {
                throw new IllegalArgumentException("Update Method '" + updateMethod + "' is not valid.");
            }

            keysUsedInOrder.add(fieldName);
        }

        if (updateAssignments == null) {
            throw new ProcessException("No update assignment found");
        }

        Update update = null;

        for (String fieldName : updateKeyNames) {
            update = update == null ? updateAssignments.whereColumn(fieldName).isEqualTo(QueryBuilder.bindMarker(fieldName))
                    : update.whereColumn(fieldName).isEqualTo(QueryBuilder.bindMarker(fieldName));
            keysUsedInOrder.add(fieldName);
        }

        return new GeneratedResult(update.build(),  keysUsedInOrder);
    }

    @Override
    public void delete(QualifiedTableName cassandraTable, org.apache.nifi.serialization.record.Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                        List<String> updateKeys) throws QueryFailureException {
        GeneratedResult result = generateDelete(cassandraTable, record, primaryKeyOverrides, updateKeys);
        PreparedStatement preparedStatement = cassandraSession.prepare(result.statement());

        BoundStatement deleteStatement = preparedStatement.bind(
                getBindValues(cassandraTable, primaryKeyOverrides, record, preparedStatement, result.keysUsed()));

        try {
            cassandraSession.execute(deleteStatement);
        } catch (QueryExecutionException | AllNodesFailedException | DriverTimeoutException qee) {
            getLogger().error("Error executing delete", qee);
            throw new QueryFailureException();
        }
    }

    @Override
    public void update(QualifiedTableName cassandraTable, org.apache.nifi.serialization.record.Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                        List<String> updateKeys, UpdateMethod updateMethod, WriteOverrides overrides) throws QueryFailureException {
        final boolean useTimestampOverride = hasTimestampOverride(overrides) && updateMethod == UpdateMethod.SET;
        GeneratedResult result = generateUpdate(cassandraTable, record, primaryKeyOverrides, updateKeys, updateMethod, resolveTtlSeconds(overrides, defaultTtl), useTimestampOverride);
        PreparedStatement preparedStatement = cassandraSession.prepare(result.statement());

        Object[] values = getBindValues(cassandraTable, primaryKeyOverrides, record, preparedStatement, result.keysUsed());
        if (useTimestampOverride) {
            setTimestampBindValue(values, preparedStatement, record, overrides);
        }
        BoundStatement statement = preparedStatement.bind(values);

        try {
            cassandraSession.execute(statement);
        } catch (QueryExecutionException | AllNodesFailedException | DriverTimeoutException qee) {
            getLogger().error("Error executing update", qee);
            throw new QueryFailureException();
        }
    }

    /**
     * Looks up each field's actual bind-marker position by name rather than assuming it matches
     * {@code keyNamesInOrder}'s own list order. That assumption held as long as every bind marker in the
     * statement corresponded 1:1 with an entry in {@code keyNamesInOrder} in the same order - which broke once
     * {@link #WRITE_TIMESTAMP_BIND_MARKER} started being inserted into UPDATE statements ahead of the SET
     * assignments (CQL requires {@code USING TIMESTAMP} immediately after the table name, before {@code SET}),
     * shifting every real field's position by one. Any position not covered by {@code keyNamesInOrder} (such as
     * that marker's) is left {@code null} here; callers that added such a marker are responsible for setting it
     * themselves afterward, by name.
     */
    private Object[] getBindValues(QualifiedTableName qualifiedTableName,
                                   Map<PrimaryKeyIdentifier, RecordPath> mappings,
                                   org.apache.nifi.serialization.record.Record record,
                                   PreparedStatement preparedStatement, List<String> keyNamesInOrder) {
        ColumnDefinitions variableDefinitions = preparedStatement.getVariableDefinitions();
        Object[] result = new Object[variableDefinitions.size()];

        for (String fieldName : keyNamesInOrder) {
            int index = variableDefinitions.firstIndexOf(fieldName);

            RecordPath path = getRecordPathOverride(qualifiedTableName, fieldName, mappings);
            Object value;
            if (path != null) {
                value = evaluateOverride(record, path);
            } else {
                value = record.getValue(fieldName);
            }
            DataType cqlType = variableDefinitions.get(index).getType();
            result[index] = convertForCqlType(value, cqlType);
        }

        return result;
    }

    /**
     * Resolves each positional query parameter to the Java type its bind marker's declared CQL type requires,
     * reusing the same {@link #convertForCqlType} conversion the write path applies to record values. This is
     * what lets a caller supply parameters as text - all Expression Language can produce - and still bind them
     * to non-text columns: the {@code Flexible*Codec}s registered with the session already accept a String for
     * the boolean, numeric, date and time types, and {@code convertForCqlType} covers uuid and timeuuid,
     * leaving {@code timestamp} (see {@link #toInstant(Object)}) as the one scalar type needing help here.
     *
     * <p>A parameter count that doesn't match the statement's bind markers fails immediately with a message
     * naming both counts, rather than reaching the driver as an unbound-marker error that doesn't say which
     * side was wrong.
     */
    private Object[] toBindValues(final PreparedStatement preparedStatement, final List<Object> parameters) {
        final ColumnDefinitions bindMarkers = preparedStatement.getVariableDefinitions();

        if (bindMarkers.size() != parameters.size()) {
            throw new IllegalArgumentException(String.format(
                    "Query declares %d bind marker(s) but %d parameter(s) were supplied",
                    bindMarkers.size(), parameters.size()));
        }

        final Object[] values = new Object[parameters.size()];

        for (int index = 0; index < parameters.size(); index++) {
            final DataType cqlType = bindMarkers.get(index).getType();
            final Object value = parameters.get(index);

            values[index] = cqlType.equals(DataTypes.TIMESTAMP)
                    ? toInstant(value)
                    : convertForCqlType(value, cqlType);
        }

        return values;
    }

    /**
     * Converts a value bound to a {@code timestamp} column into the {@link Instant} the driver's own codec
     * expects. Unlike the other scalar types, {@code timestamp} has no {@code Flexible*Codec} registered to
     * coerce a compatible-but-non-exact value, so an ISO-8601 rendering - the natural form for a parameter
     * supplied as text - would otherwise fail with the driver's opaque {@code CodecNotFoundException}.
     */
    static Instant toInstant(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Instant instantValue) {
            return instantValue;
        } else if (value instanceof java.util.Date dateValue) {
            return dateValue.toInstant();
        } else if (value instanceof Number numberValue) {
            return Instant.ofEpochMilli(numberValue.longValue());
        }

        final String text = value.toString().trim();

        try {
            return Instant.parse(text);
        } catch (final DateTimeParseException e) {
            try {
                return Instant.ofEpochMilli(Long.parseLong(text));
            } catch (final NumberFormatException ignored) {
                throw new IllegalArgumentException(String.format(
                        "Value '%s' cannot be bound to a timestamp column: expected an ISO-8601 instant such as "
                                + "2026-08-07T14:30:00Z, or a count of milliseconds since the epoch", text), e);
            }
        }
    }

    private Object evaluateOverride(Record record, RecordPath path) {
        RecordPathResult result = path.evaluate(record);
        List<FieldValue> valueList = result.getSelectedFields().toList();

        if (valueList.isEmpty()) {
            throw new IllegalArgumentException(String.format("%s evaluated to no values.", path.getPath()));
        } else if (valueList.size() > 1) {
            throw new IllegalArgumentException(String.format("%s evaluated to more than one value.", path.getPath()));
        }

        return valueList.get(0).getValue();
    }

    private RecordPath getRecordPathOverride(QualifiedTableName tableName, String fieldName,
                                          Map<PrimaryKeyIdentifier, RecordPath> overrides) {
        final String cleanFieldName = CqlIdentifier.fromCql(fieldName).asInternal();
        final String keyspace = tableName.isQualified() ? tableName.keyspace() : defaultKeyspace;

        Optional<Map.Entry<PrimaryKeyIdentifier, RecordPath>> identifier = overrides
                .entrySet()
                .stream()
                .filter((e) -> e.getKey().keyspace().equals(keyspace)
                        && e.getKey().tableName().equals(tableName.table())
                        && CqlIdentifier.fromCql(e.getKey().fieldName()).asInternal().equals(cleanFieldName))
                .findFirst();

        return identifier.map(Map.Entry::getValue).orElse(null);
    }

    /**
     * Fills in the slot reserved for {@link #WRITE_TIMESTAMP_BIND_MARKER} directly in the same array used for
     * the single {@link PreparedStatement#bind(Object...)} call, rather than binding it separately afterward via
     * a named setter on the resulting {@link BoundStatement} - the latter looked correct but did not reliably
     * take effect for UPDATE statements when verified against a real cluster.
     */
    private void setTimestampBindValue(Object[] values, PreparedStatement preparedStatement, org.apache.nifi.serialization.record.Record record, WriteOverrides overrides) {
        int index = preparedStatement.getVariableDefinitions().firstIndexOf(WRITE_TIMESTAMP_BIND_MARKER);
        values[index] = toEpochMicros(record.getValue(overrides.timestampField()));
    }

    /**
     * Converts a nested {@code Record} or a raw {@code Map} value into a {@link UdtValue} when the target
     * column is a Cassandra User Defined Type, recursing into any UDT-typed fields it contains (both are
     * accepted since {@code DataTypeUtils#toRecord} treats them as equally valid representations of RECORD);
     * converts each element of a {@code List}, {@code Set}, or {@code Object[]} value the same way when the
     * target column is a Cassandra list or set (including a list/set of UDTs) - {@code Object[]} is accepted
     * alongside {@code List}/{@code Set} since it's NiFi's own canonical ARRAY representation
     * ({@code DataTypeUtils#toArray} always returns one); and converts each value of a {@code Map} value the
     * same way when the target column is a Cassandra map (including a map with UDT values). For uuid and
     * timeuuid columns, the value is normalized via {@link DataTypeUtils} so that a compatible but
     * non-exact value - for example a {@code String} representation of a UUID - still binds correctly; for
     * timeuuid specifically, the normalized UUID is also checked to be version 1 (time-based), since that's
     * a hard requirement of the CQL type and failing here produces a clear, attributable error instead of
     * the driver's own confusing {@code CodecNotFoundException} for a non-compliant value.
     * CHAR and the remaining scalar types whose CQL type requires an exact Java type the driver's default
     * codec won't coerce into (boolean and the numeric types) are not handled here: each has a
     * {@code Flexible*Codec}/{@link org.apache.nifi.service.cassandra.mapping.CharacterCodec} registered
     * with the session so a compatible but non-exact value binds directly. Every other value is passed
     * through unchanged, since the driver's default codecs already handle it.
     */
    private Object convertForCqlType(final Object value, final DataType cqlType) {
        if (value == null) {
            return null;
        }

        // A RECORD-typed value is normally a nested Record, but DataTypeUtils#toRecord also accepts a raw Map
        // as an equally valid representation, so both are converted into a UdtValue the same way here.
        if (cqlType instanceof UserDefinedType udtType
                && (value instanceof org.apache.nifi.serialization.record.Record || value instanceof Map<?, ?>)) {
            final Function<String, Object> fieldValueLookup = value instanceof org.apache.nifi.serialization.record.Record nestedRecord
                    ? nestedRecord::getValue
                    : ((Map<?, ?>) value)::get;

            UdtValue udtValue = udtType.newValue();
            final List<String> fieldNames = udtType.getFieldNames().stream().map(name -> name.asInternal()).toList();

            for (int i = 0; i < fieldNames.size(); i++) {
                final String fieldName = fieldNames.get(i);
                final DataType fieldCqlType = udtType.getFieldTypes().get(i);
                final Object fieldValue = convertForCqlType(fieldValueLookup.apply(fieldName), fieldCqlType);
                try {
                    // A null field is set directly against its declared CQL type rather than through a codec
                    // lookup. Resolving the codec from the value's runtime class cannot work when there is no
                    // value: the lookup becomes [<field type> <-> java.lang.Object], which no codec satisfies,
                    // and that made any UDT carrying an absent or optional field unwritable.
                    udtValue = fieldValue == null
                            ? udtValue.setToNull(fieldName)
                            : udtValue.set(fieldName, fieldValue, (Class<Object>) fieldValue.getClass());
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }

            return udtValue;
        }

        // An ARRAY-typed value is normally a List, but Object[] is NiFi's own canonical representation
        // (DataTypeUtils#toArray always returns one), so both are accepted here for list and set columns.
        if (cqlType instanceof ListType listType && (value instanceof List<?> || value instanceof Object[])) {
            final DataType elementCqlType = listType.getElementType();
            return toElementStream(value).map(element -> convertForCqlType(element, elementCqlType)).toList();
        }

        if (cqlType instanceof SetType setType && (value instanceof Set<?> || value instanceof Object[])) {
            final DataType elementCqlType = setType.getElementType();
            return toElementStream(value).map(element -> convertForCqlType(element, elementCqlType)).collect(Collectors.toSet());
        }

        if (value instanceof Map<?, ?> map && cqlType instanceof MapType mapType) {
            final DataType keyCqlType = mapType.getKeyType();
            final DataType valueCqlType = mapType.getValueType();
            final Map<Object, Object> converted = new HashMap<>();

            for (final Map.Entry<?, ?> entry : map.entrySet()) {
                converted.put(convertForCqlType(entry.getKey(), keyCqlType), convertForCqlType(entry.getValue(), valueCqlType));
            }

            return converted;
        }

        if (cqlType.equals(DataTypes.UUID)) {
            return DataTypeUtils.toUUID(value);
        }

        // The driver's own codec already rejects a non-version-1 UUID bound to a timeuuid column, but only
        // via a confusing CodecNotFoundException ("Codec not found for requested operation: [TIMEUUID <->
        // java.util.UUID]") that reads like a driver/configuration problem rather than a data problem.
        // Checking here instead produces a clear, attributable error before the value ever reaches bind().
        if (cqlType.equals(DataTypes.TIMEUUID)) {
            final UUID uuid = DataTypeUtils.toUUID(value);
            if (uuid.version() != 1) {
                throw new IllegalArgumentException(String.format(
                        "Value '%s' is not a valid timeuuid: version %d, but timeuuid columns require a version 1 (time-based) UUID",
                        uuid, uuid.version()));
            }
            return uuid;
        }

        return value;
    }

    private static Stream<?> toElementStream(final Object value) {
        return (value instanceof Object[] array) ? Arrays.stream(array) : ((Collection<?>) value).stream();
    }

    @Override
    public void update(QualifiedTableName cassandraTable, List<org.apache.nifi.serialization.record.Record> records, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                        List<String> updateKeys, UpdateMethod updateMethod, CqlBatchType batchType,
                        WriteOverrides overrides) throws QueryFailureException {
        if (records == null || records.isEmpty()) {
            return;
        }

        final boolean useTimestampOverride = hasTimestampOverride(overrides) && updateMethod == UpdateMethod.SET;
        BatchStatementBuilder builder = BatchStatement.builder(toDriverBatchType(batchType));

        GeneratedResult result = generateUpdate(cassandraTable, records.get(0), primaryKeyOverrides, updateKeys, updateMethod, resolveTtlSeconds(overrides, defaultTtl), useTimestampOverride);
        PreparedStatement preparedStatement = cassandraSession.prepare(result.statement());

        for (org.apache.nifi.serialization.record.Record record : records) {
            Object[] values = getBindValues(cassandraTable, primaryKeyOverrides, record, preparedStatement, result.keysUsed());
            if (useTimestampOverride) {
                setTimestampBindValue(values, preparedStatement, record, overrides);
            }
            builder.addStatement(preparedStatement.bind(values));
        }

        try {
            cassandraSession.execute(builder.build());
        } catch (QueryExecutionException | AllNodesFailedException | DriverTimeoutException qee) {
            getLogger().error("Error executing batch update", qee);
            throw new QueryFailureException();
        }
    }

    @Override
    public PrimaryKey getMetadata(QualifiedTableName table) {
        cacheMetadata(table);
        return tableMetadataCache.get(resolveKeyspace(table));
    }

    record GeneratedResult(SimpleStatement statement, List<String> keysUsed) {

    }
}
