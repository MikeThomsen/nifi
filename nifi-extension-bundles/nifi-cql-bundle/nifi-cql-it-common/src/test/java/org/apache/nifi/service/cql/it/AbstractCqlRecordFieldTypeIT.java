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
package org.apache.nifi.service.cql.it;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.service.cql.api.service.CQLQueryCallback;
import org.apache.nifi.service.cql.api.service.QueryOverrides;
import org.apache.nifi.service.cql.api.service.WriteOverrides;
import org.apache.nifi.service.cql.api.metadata.QualifiedTableName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.api.TestInstance;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Stream;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link CQLExecutionService#insert(String, org.apache.nifi.serialization.record.Record, WriteOverrides)}
 * and {@link CQLExecutionService#query(String, boolean, List, CQLQueryCallback, QueryOverrides)} once per
 * {@link RecordFieldType},
 * excluding CHOICE, which has no natural CQL column type since it represents a union of possible types rather
 * than a single one, against whichever backend the concrete subclass wires up. Every case follows the same
 * shape: write a record, assert the write succeeds, read the row back, and assert the value comes back
 * correctly. Each creates a dedicated table whose one non-key column is a CQL type that naturally fits
 * the RecordFieldType under test, or - for RECORD and MAP, whose natural fit needs more than a single column
 * type - a User Defined Type.
 *
 * <p>Most of that matrix is the {@code singleColumnTypes} table driving {@link #testSingleColumnType}, one row
 * per pairing, so the coverage can be read as a grid rather than reconstructed by scrolling through two dozen
 * near-identical methods. Types whose handling needs more than a value and a comparison - collections, UDTs,
 * and the cases asserting a rejection - keep their own methods, since the row shape would obscure what they
 * actually check.
 *
 * <p>{@code CassandraCQLExecutionService} (the base implementation both Cassandra's and ScyllaDB's session
 * providers share) binds {@code Record.getValue(fieldName)} directly to the prepared statement without any
 * type coercion beyond what {@code convertForCqlType} normalizes, so several cases here deliberately use a
 * non-canonical input value (a {@code String}, a mismatched {@code Number}, etc.) to prove that normalization
 * actually happens, then confirm the stored value is correct by reading it back. Those are the rows whose
 * {@code written} and {@code expected} columns differ.
 * <p>
 * Setup is a plain protected method rather than a JUnit lifecycle callback for the same reason documented on
 * {@link AbstractCqlCrudIT}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractCqlRecordFieldTypeIT {

    protected CQLExecutionService sessionProvider;

    private CqlSession session;

    private String keyspace;

    /**
     * @return a fresh, unconfigured instance of the backend implementation under test
     */
    protected abstract CQLExecutionService newSessionProvider();

    protected void initializeSessionProvider(final CqlConnectionInfo connectionInfo) throws Exception {
        this.session = connectionInfo.session();
        this.keyspace = connectionInfo.keyspace();
        this.sessionProvider = CqlServiceRunner.forService(newSessionProvider())
                .withConnection(connectionInfo)
                .enable();
    }

    /** Idempotent DDL, retried on the failures that say nothing about whether it ran - see {@link CqlDdl}. */
    private void executeDdlWithRetry(final String cql) {
        CqlDdl.executeWithRetry(session, cql);
    }

    private void createTable(final String tableName, final String cqlColumnType) {
        executeDdlWithRetry(String.format("create table if not exists %s.%s (id int primary key, value_field %s)", keyspace, tableName, cqlColumnType));
    }

    private RecordSchema schemaFor(final RecordField valueField) {
        return new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType()), valueField));
    }

    /**
     * Reads back exactly one row via {@link CQLExecutionService#query}, asserting the query itself doesn't
     * throw and that exactly one row came back, then returns it for the caller to assert on.
     */
    private org.apache.nifi.serialization.record.Record readBack(final String tableName, final String columns, final int id) {
        final CollectingCqlQueryCallback callback = new CollectingCqlQueryCallback();

        assertDoesNotThrow(() -> sessionProvider.query(
                String.format("select %s from %s.%s where id = %d", columns, keyspace, tableName, id), null, callback, QueryOverrides.NONE));

        final List<org.apache.nifi.serialization.record.Record> results = callback.getRecords();
        assertEquals(1, results.size(), () -> "Expected exactly one row back from " + tableName);
        return results.getFirst();
    }

    /**
     * The single-column type matrix: one row per (record field type, CQL column type) pairing, each writing a
     * value and reading it back.
     *
     * <p>{@code written} and {@code expected} differ for the rows that exist to prove coercion happens - a
     * value handed to the service in one Java type must come back in the type the column actually holds. Where
     * they are the same object the row is a plain round-trip.
     *
     * <p>Types whose handling needs more than a value and a comparison - collections, UDTs, and the
     * timeuuid rejection - keep their own methods below; forcing them into this shape would hide what they
     * assert.
     */
    static Stream<Arguments> singleColumnTypes() {
        final Instant timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        final UUID uuid = UUID.randomUUID();
        final UUID timeUuid = Uuids.timeBased();
        final Date sqlDate = Date.valueOf(LocalDate.of(2024, 3, 15));
        final Time sqlTime = Time.valueOf(LocalTime.of(13, 45, 30));
        final BigDecimal decimal = new BigDecimal("123.456");
        final Map<String, String> map = Map.of("key1", "value1", "key2", "value2");
        // Bound once and reused for both the written and the expected value: calling now() twice would compare
        // two different instants, which is a round-trip failure that has nothing to do with the round trip.
        final LocalDate today = LocalDate.now();
        final LocalTime timeOfDay = LocalTime.now();

        return Stream.of(
                arguments("BOOLEAN -> boolean", "boolean_test", "boolean", RecordFieldType.BOOLEAN.getDataType(), Boolean.TRUE, Boolean.TRUE),
                arguments("BYTE -> tinyint", "byte_test", "tinyint", RecordFieldType.BYTE.getDataType(), (byte) 42, (byte) 42),
                arguments("SHORT -> smallint", "short_test", "smallint", RecordFieldType.SHORT.getDataType(), (short) 1234, (short) 1234),
                arguments("INT -> int", "int_test", "int", RecordFieldType.INT.getDataType(), 123456, 123456),
                arguments("LONG -> bigint", "long_test", "bigint", RecordFieldType.LONG.getDataType(), 123456789012345L, 123456789012345L),
                arguments("BIGINT -> varint", "bigint_test", "varint", RecordFieldType.BIGINT.getDataType(), BigInteger.valueOf(123456789L), BigInteger.valueOf(123456789L)),
                arguments("FLOAT -> float", "float_test", "float", RecordFieldType.FLOAT.getDataType(), 1.5f, 1.5f),
                arguments("DOUBLE -> double", "double_test", "double", RecordFieldType.DOUBLE.getDataType(), 1.5d, 1.5d),
                arguments("DECIMAL -> decimal", "decimal_test", "decimal", RecordFieldType.DECIMAL.getDataType(), decimal, decimal),

                // DECIMAL is the only scalar-adjacent type (besides ENUM) with its own DataType subclass, so this
                // row exercises DecimalDataType specifically. CQL's decimal has no fixed precision or scale of its
                // own - it stores an arbitrary-precision unscaled value - so the precision/scale here is NiFi-side
                // schema metadata that the value must simply conform to.
                arguments("DECIMAL(10,2) -> decimal", "decimal_precision_scale_test", "decimal",
                        RecordFieldType.DECIMAL.getDecimalDataType(10, 2), new BigDecimal("12345678.90"), new BigDecimal("12345678.90")),

                // CQL timestamp has millisecond resolution, so the written value is truncated up front to make
                // the round-trip comparison exact.
                arguments("TIMESTAMP -> timestamp", "timestamp_test", "timestamp", RecordFieldType.TIMESTAMP.getDataType(), timestamp, timestamp),
                arguments("DATE -> date", "date_test", "date", RecordFieldType.DATE.getDataType(), today, today),
                arguments("TIME -> time", "time_test", "time", RecordFieldType.TIME.getDataType(), timeOfDay, timeOfDay),
                arguments("UUID -> uuid", "uuid_test", "uuid", RecordFieldType.UUID.getDataType(), uuid, uuid),
                arguments("UUID (v1) -> timeuuid", "timeuuid_test", "timeuuid", RecordFieldType.UUID.getDataType(), timeUuid, timeUuid),
                arguments("CHAR -> text", "char_test", "text", RecordFieldType.CHAR.getDataType(), "A", "A"),
                arguments("ENUM -> text", "enum_test", "text", RecordFieldType.ENUM.getDataType(), "VALUE_ONE", "VALUE_ONE"),
                arguments("STRING -> text", "string_test", "text", RecordFieldType.STRING.getDataType(), "hello world", "hello world"),
                arguments("MAP -> map<text, text>", "map_test", "map<text, text>",
                        RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()), map, map),

                // Coercion rows: written in one Java type, expected back in the type the column holds. Reading
                // back the coerced type - rather than the type handed in - is what proves the conversion ran.
                arguments("BOOLEAN <- String", "boolean_coercion_test", "boolean", RecordFieldType.BOOLEAN.getDataType(), "true", Boolean.TRUE),
                arguments("UUID <- String", "uuid_coercion_test", "uuid", RecordFieldType.UUID.getDataType(), uuid.toString(), uuid),
                arguments("CHAR <- Character", "char_coercion_test", "text", RecordFieldType.CHAR.getDataType(), 'A', "A"),
                arguments("INT <- String", "int_from_string_test", "int", RecordFieldType.INT.getDataType(), "123456", 123456),
                // A Short is a valid Number but not the exact Integer type the driver's INT codec requires.
                arguments("INT <- Short", "int_from_short_test", "int", RecordFieldType.INT.getDataType(), (short) 1234, 1234),

                // java.sql.Date/Time bind through JavaSQLDateCodec and JavaSQLTimeCodec, but reads go through the
                // driver's default DATE/TIME codecs: registering those codecs adds a way to bind the java.sql
                // type, it does not replace the driver's default for an untyped read like Row.getObject(int).
                arguments("DATE <- java.sql.Date", "date_coercion_test", "date", RecordFieldType.DATE.getDataType(), sqlDate, sqlDate.toLocalDate()),
                arguments("TIME <- java.sql.Time", "time_coercion_test", "time", RecordFieldType.TIME.getDataType(), sqlTime, sqlTime.toLocalTime()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("singleColumnTypes")
    @DisplayName("A record field writes to its CQL column and reads back as the value the column holds")
    void testSingleColumnType(final String description, final String tableName, final String cqlColumnType,
                              final DataType dataType, final Object written, final Object expected) {
        createTable(tableName, cqlColumnType);
        final RecordSchema schema = schemaFor(new RecordField("value_field", dataType));
        final MapRecord record = new MapRecord(schema, Map.of("id", 1, "value_field", written));

        assertDoesNotThrow(() -> sessionProvider.insert(new QualifiedTableName(null, tableName), record, Map.of(), WriteOverrides.NONE));

        assertEquals(expected, readBack(tableName, "value_field", 1).getValue("value_field"));
    }

    @Test
    @DisplayName("A record with a non-version-1 UUID field is rejected, not silently written, when targeting a timeuuid column")
    void testTimeUuidRejectsNonVersion1Uuid() {
        // A timeuuid column requires a genuine version 1 (time-based) UUID - the driver's own codec would
        // otherwise reject this with a confusing CodecNotFoundException ("Codec not found for requested
        // operation: [TIMEUUID <-> java.util.UUID]") that reads like a driver/configuration problem rather
        // than a data problem, so convertForCqlType checks and fails clearly before ever reaching bind().
        createTable("timeuuid_reject_test", "timeuuid");
        final RecordSchema schema = schemaFor(new RecordField("value_field", RecordFieldType.UUID.getDataType()));
        final UUID randomUuid = UUID.randomUUID();
        final MapRecord record = new MapRecord(schema, Map.of("id", 1, "value_field", randomUuid));

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> sessionProvider.insert(new QualifiedTableName(null, "timeuuid_reject_test"), record, Map.of(), WriteOverrides.NONE));

        assertTrue(exception.getMessage().contains(randomUuid.toString()),
                "Expected the exception message to name the offending value, but was: " + exception.getMessage());
    }

    @Test
    @DisplayName("A record with an ARRAY of STRING field holding a List writes to and reads back from a list<text> column")
    void testArray() {
        createTable("array_test", "list<text>");
        final RecordSchema schema = schemaFor(
                new RecordField("value_field", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())));
        final List<String> expected = List.of("a", "b", "c");
        final MapRecord record = new MapRecord(schema, Map.of("id", 1, "value_field", expected));

        assertDoesNotThrow(() -> sessionProvider.insert(new QualifiedTableName(null, "array_test"), record, Map.of(), WriteOverrides.NONE));

        assertEquals(expected, readBack("array_test", "value_field", 1).getValue("value_field"));
    }

    @Test
    @DisplayName("A record with an ARRAY of STRING field holding an Object[] writes to and reads back from a list<text> column")
    void testArrayFromObjectArrayIsCoerced() {
        // Object[] is NiFi's own canonical ARRAY representation (DataTypeUtils#toArray always returns one),
        // unlike the List used by testArray, so this exercises that path specifically.
        createTable("array_object_array_test", "list<text>");
        final RecordSchema schema = schemaFor(
                new RecordField("value_field", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())));
        final MapRecord record = new MapRecord(schema, Map.of("id", 1, "value_field", new Object[] {"a", "b", "c"}));

        assertDoesNotThrow(() -> sessionProvider.insert(new QualifiedTableName(null, "array_object_array_test"), record, Map.of(), WriteOverrides.NONE));

        assertEquals(List.of("a", "b", "c"), readBack("array_object_array_test", "value_field", 1).getValue("value_field"));
    }

    @Test
    @DisplayName("A record with an ARRAY of STRING field holding a Set writes to and reads back from a set<text> column")
    void testSetOfString() {
        createTable("set_test", "set<text>");
        final RecordSchema schema = schemaFor(
                new RecordField("value_field", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())));
        final Set<String> expected = Set.of("a", "b", "c");
        final MapRecord record = new MapRecord(schema, Map.of("id", 1, "value_field", expected));

        assertDoesNotThrow(() -> sessionProvider.insert(new QualifiedTableName(null, "set_test"), record, Map.of(), WriteOverrides.NONE));

        assertEquals(expected, readBack("set_test", "value_field", 1).getValue("value_field"));
    }

    @Test
    @DisplayName("A record with a MAP of RECORD field writes a map of UDT values to and reads it back")
    void testMapOfAddressUserDefinedType() {
        executeDdlWithRetry("create type if not exists " + keyspace + ".address_map_item (street_address text, state text, zip_code int)");
        executeDdlWithRetry(String.format("create table if not exists %s.address_map_test (id int primary key, addresses map<text, frozen<address_map_item>>)", keyspace));

        final RecordSchema addressSchema = new SimpleRecordSchema(List.of(
                new RecordField("street_address", RecordFieldType.STRING.getDataType()),
                new RecordField("state", RecordFieldType.STRING.getDataType()),
                new RecordField("zip_code", RecordFieldType.INT.getDataType())
        ));
        final RecordSchema schema = schemaFor(new RecordField("addresses",
                RecordFieldType.MAP.getMapDataType(RecordFieldType.RECORD.getRecordDataType(addressSchema))));

        final MapRecord home = new MapRecord(addressSchema, Map.of("street_address", "123 Main St", "state", "NC", "zip_code", 27601));
        final MapRecord work = new MapRecord(addressSchema, Map.of("street_address", "456 Elm St", "state", "SC", "zip_code", 29401));
        final MapRecord record = new MapRecord(schema, Map.of("id", 1, "addresses", Map.of("home", home, "work", work)));

        assertDoesNotThrow(() -> sessionProvider.insert(new QualifiedTableName(null, "address_map_test"), record, Map.of(), WriteOverrides.NONE));

        final Object addresses = readBack("address_map_test", "addresses", 1).getValue("addresses");
        assertInstanceOf(Map.class, addresses);
        @SuppressWarnings("unchecked")
        final Map<String, UdtValue> addressMap = (Map<String, UdtValue>) addresses;
        assertEquals("123 Main St", addressMap.get("home").getString("street_address"));
        assertEquals("456 Elm St", addressMap.get("work").getString("street_address"));
    }

    @Test
    @DisplayName("A record with a RECORD field for a Home Address writes a UDT and reads it back")
    void testHomeAddressUserDefinedType() {
        executeDdlWithRetry("create type if not exists " + keyspace + ".home_address (street_address text, state text, zip_code int)");
        executeDdlWithRetry(String.format("create table if not exists %s.home_address_test (id int primary key, home_address frozen<home_address>)", keyspace));

        final RecordSchema addressSchema = new SimpleRecordSchema(List.of(
                new RecordField("street_address", RecordFieldType.STRING.getDataType()),
                new RecordField("state", RecordFieldType.STRING.getDataType()),
                new RecordField("zip_code", RecordFieldType.INT.getDataType())
        ));
        final RecordSchema schema = schemaFor(new RecordField("home_address", RecordFieldType.RECORD.getRecordDataType(addressSchema)));

        // A plain nested MapRecord, exactly like what a RecordReader (JSON, Avro, etc.) would produce for a
        // nested object - the session provider is responsible for converting this into the UdtValue the
        // DataStax driver's codec requires.
        final MapRecord homeAddress = new MapRecord(addressSchema, Map.of(
                "street_address", "123 Main St",
                "state", "NC",
                "zip_code", 27601));

        final MapRecord record = new MapRecord(schema, Map.of("id", 1, "home_address", homeAddress));

        assertDoesNotThrow(() -> sessionProvider.insert(new QualifiedTableName(null, "home_address_test"), record, Map.of(), WriteOverrides.NONE));

        final Object readValue = readBack("home_address_test", "home_address", 1).getValue("home_address");
        assertInstanceOf(UdtValue.class, readValue);
        final UdtValue udtValue = (UdtValue) readValue;
        assertEquals("123 Main St", udtValue.getString("street_address"));
        assertEquals("NC", udtValue.getString("state"));
        assertEquals(27601, udtValue.getInt("zip_code"));
    }

    @Test
    @DisplayName("A record with a RECORD field holding a raw Map writes a UDT and reads it back")
    void testHomeAddressFromRawMapIsCoerced() {
        // A raw Map is the other representation DataTypeUtils#toRecord accepts for a RECORD field, alongside
        // the nested Record used by testHomeAddressUserDefinedType, so this exercises that path specifically.
        executeDdlWithRetry("create type if not exists " + keyspace + ".address_from_map_item (street_address text, state text, zip_code int)");
        executeDdlWithRetry(String.format(
                "create table if not exists %s.address_from_map_test (id int primary key, home_address frozen<address_from_map_item>)", keyspace));

        final RecordSchema addressSchema = new SimpleRecordSchema(List.of(
                new RecordField("street_address", RecordFieldType.STRING.getDataType()),
                new RecordField("state", RecordFieldType.STRING.getDataType()),
                new RecordField("zip_code", RecordFieldType.INT.getDataType())
        ));
        final RecordSchema schema = schemaFor(new RecordField("home_address", RecordFieldType.RECORD.getRecordDataType(addressSchema)));

        final Map<String, Object> homeAddress = Map.of("street_address", "123 Main St", "state", "NC", "zip_code", 27601);
        final MapRecord record = new MapRecord(schema, Map.of("id", 1, "home_address", homeAddress));

        assertDoesNotThrow(() -> sessionProvider.insert(new QualifiedTableName(null, "address_from_map_test"), record, Map.of(), WriteOverrides.NONE));

        final Object readValue = readBack("address_from_map_test", "home_address", 1).getValue("home_address");
        assertInstanceOf(UdtValue.class, readValue);
        final UdtValue udtValue = (UdtValue) readValue;
        assertEquals("123 Main St", udtValue.getString("street_address"));
        assertEquals("NC", udtValue.getString("state"));
        assertEquals(27601, udtValue.getInt("zip_code"));
    }

    @Test
    @DisplayName("A record with a UDT nested inside another UDT writes to and reads back without any UDT-specific code")
    void testPersonWithNestedAddressUserDefinedType() {
        // Deliberately different names, field count, casing, and nesting depth than testHomeAddressUserDefinedType:
        // this proves the RECORD-to-UdtValue conversion is driven entirely by the driver's live UDT metadata
        // rather than any UDT-specific code, since it also has to recurse into a UDT nested inside another UDT.
        // Quoting "firstName"/"lastName"/"zipCode" preserves their camelCase spelling; CQL would otherwise fold
        // unquoted identifiers to lowercase.
        executeDdlWithRetry("create type if not exists " + keyspace + ".address (street text, state text, \"zipCode\" int)");
        executeDdlWithRetry("create type if not exists " + keyspace + ".person (\"firstName\" text, \"lastName\" text, address frozen<address>)");
        executeDdlWithRetry(String.format("create table if not exists %s.person_test (id int primary key, person frozen<person>)", keyspace));

        final RecordSchema addressSchema = new SimpleRecordSchema(List.of(
                new RecordField("street", RecordFieldType.STRING.getDataType()),
                new RecordField("state", RecordFieldType.STRING.getDataType()),
                new RecordField("zipCode", RecordFieldType.INT.getDataType())
        ));
        final RecordSchema personSchema = new SimpleRecordSchema(List.of(
                new RecordField("firstName", RecordFieldType.STRING.getDataType()),
                new RecordField("lastName", RecordFieldType.STRING.getDataType()),
                new RecordField("address", RecordFieldType.RECORD.getRecordDataType(addressSchema))
        ));
        final RecordSchema schema = schemaFor(new RecordField("person", RecordFieldType.RECORD.getRecordDataType(personSchema)));

        final MapRecord address = new MapRecord(addressSchema, Map.of(
                "street", "123 Main St",
                "state", "NC",
                "zipCode", 27601));
        final MapRecord person = new MapRecord(personSchema, Map.of(
                "firstName", "John",
                "lastName", "Doe",
                "address", address));
        final MapRecord record = new MapRecord(schema, Map.of("id", 1, "person", person));

        assertDoesNotThrow(() -> sessionProvider.insert(new QualifiedTableName(null, "person_test"), record, Map.of(), WriteOverrides.NONE));

        final Object readValue = readBack("person_test", "person", 1).getValue("person");
        assertInstanceOf(UdtValue.class, readValue);
        final UdtValue personValue = (UdtValue) readValue;
        assertEquals("John", personValue.getString("firstName"));
        assertEquals("Doe", personValue.getString("lastName"));
        final UdtValue addressValue = personValue.getUdtValue("address");
        assertEquals("123 Main St", addressValue.getString("street"));
        assertEquals("NC", addressValue.getString("state"));
        assertEquals(27601, addressValue.getInt("zipCode"));
    }

    @Test
    @DisplayName("A record with an ARRAY of RECORD field writes a list of UDTs to and reads it back")
    void testArrayOfAddressUserDefinedType() {
        executeDdlWithRetry("create type if not exists " + keyspace + ".address_item (street_address text, state text, zip_code int)");
        executeDdlWithRetry(String.format("create table if not exists %s.address_array_test (id int primary key, addresses list<frozen<address_item>>)", keyspace));

        final RecordSchema addressSchema = new SimpleRecordSchema(List.of(
                new RecordField("street_address", RecordFieldType.STRING.getDataType()),
                new RecordField("state", RecordFieldType.STRING.getDataType()),
                new RecordField("zip_code", RecordFieldType.INT.getDataType())
        ));
        final RecordSchema schema = schemaFor(new RecordField("addresses",
                RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(addressSchema))));

        final MapRecord addressOne = new MapRecord(addressSchema, Map.of("street_address", "123 Main St", "state", "NC", "zip_code", 27601));
        final MapRecord addressTwo = new MapRecord(addressSchema, Map.of("street_address", "456 Elm St", "state", "SC", "zip_code", 29401));
        final MapRecord record = new MapRecord(schema, Map.of("id", 1, "addresses", List.of(addressOne, addressTwo)));

        assertDoesNotThrow(() -> sessionProvider.insert(new QualifiedTableName(null, "address_array_test"), record, Map.of(), WriteOverrides.NONE));

        final Object addresses = readBack("address_array_test", "addresses", 1).getValue("addresses");
        assertInstanceOf(List.class, addresses);
        final List<?> addressList = (List<?>) addresses;
        assertEquals(2, addressList.size());
        assertInstanceOf(UdtValue.class, addressList.get(0));
        assertEquals("123 Main St", ((UdtValue) addressList.get(0)).getString("street_address"));
        assertEquals("456 Elm St", ((UdtValue) addressList.get(1)).getString("street_address"));
    }

}
