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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.service.cql.api.metadata.PrimaryKeyIdentifier;
import org.apache.nifi.service.cql.api.metadata.QualifiedTableName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit coverage for three write-path defects, none of which needs a cluster - the point being that none of
 * them ever required one, which is why Docker-gated ITs were the wrong place to be looking for them.
 *
 * <p>Each test asserts the <em>intended</em> behaviour, so it fails while its defect is present and becomes a
 * regression test the moment the defect is fixed.
 */
public class CassandraCQLExecutionServiceWritePathTest {

    private final CassandraCQLExecutionService service = new CassandraCQLExecutionService();

    private static RecordSchema schemaOf(final RecordField... fields) {
        return new SimpleRecordSchema(List.of(fields));
    }

    // ------------------------------------------------------------------ delete() bind markers

    /**
     * Binding is {@code delete()}'s job, and it can only bind what {@code generateDelete} tells it to: the
     * ordered list of keys each of the statement's bind markers corresponds to.
     */
    @Test
    @DisplayName("generateDelete reports the keys its bind markers correspond to, so delete() can bind them")
    public void testGeneratedDeleteReportsItsBindMarkers() {
        final RecordSchema schema = schemaOf(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("region", RecordFieldType.STRING.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType()));
        final Record record = new MapRecord(schema, Map.of("id", 7, "region", "us-east", "name", "seven"));

        final CassandraCQLExecutionService.GeneratedResult result =
                service.generateDelete(new QualifiedTableName("ks", "t"), record, Map.of(), List.of("id", "region"));

        final String cql = result.statement().getQuery();
        assertTrue(cql.contains(":id") && cql.contains(":region"),
                () -> "expected a bind marker per delete key, got: " + cql);

        assertEquals(List.of("id", "region"), result.keysUsed(),
                () -> "the keys backing the bind markers in: " + cql);
    }

    /**
     * A delete key resolved only through a {@code primaryKeyOverrides} RecordPath, with no same-named record
     * field, must still be accepted: {@code generateUpdate} already treats such a key as resolvable from the
     * override alone, and {@code generateDelete} must behave the same way.
     */
    @Test
    @DisplayName("A delete key supplied only by a primary key override is accepted, not rejected as missing")
    public void testGeneratedDeleteAcceptsKeyResolvedOnlyByOverride() {
        final RecordSchema schema = schemaOf(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("created", RecordFieldType.TIMESTAMP.getDataType()));
        final Record record = new MapRecord(schema, Map.of("id", 7));

        // 'created_date' is not a record field - it exists only as a RecordPath override on this table.
        final Map<PrimaryKeyIdentifier, RecordPath> overrides = Map.of(
                new PrimaryKeyIdentifier("ks", "t", "created_date"), RecordPath.compile("/created"));

        final CassandraCQLExecutionService.GeneratedResult result =
                service.generateDelete(new QualifiedTableName("ks", "t"), record, overrides, List.of("id", "created_date"));

        assertEquals(List.of("id", "created_date"), result.keysUsed());
        assertTrue(result.statement().getQuery().contains(":created_date"),
                () -> "expected the override-resolved key to get a bind marker, got: " + result.statement().getQuery());
    }

    // ----------------------------------------------------------------- UDT null fields

    /**
     * A UDT field holding a null value has no runtime class to resolve a codec from, so it must be set via
     * {@code UdtValue.setToNull} rather than through the same codec-lookup path a non-null value uses.
     */
    @Test
    @DisplayName("A UDT with a null field converts instead of failing the codec lookup")
    public void testUdtWithNullFieldIsConvertible() {
        final UserDefinedType addressType = new UserDefinedTypeBuilder("ks", "addr")
                .withField("street", DataTypes.TEXT)
                .withField("state", DataTypes.TEXT)
                .withField("zip", DataTypes.INT)
                .build();

        final Map<String, Object> address = new HashMap<>();
        address.put("street", "1 Main St");
        address.put("state", null);
        address.put("zip", 12345);

        final Object converted = convertForCqlType(address, addressType);

        assertNotNull(converted);
        assertTrue(converted instanceof UdtValue, () -> "expected a UdtValue, got " + converted.getClass());

        final UdtValue udtValue = (UdtValue) converted;
        assertEquals("1 Main St", udtValue.getString("street"));
        assertTrue(udtValue.isNull(CqlIdentifier.fromInternal("state")), "the null field should round-trip as null");
        assertEquals(12345, udtValue.getInt("zip"));
    }

    /**
     * Same defect reached through a nested {@code Record} rather than a raw {@code Map}, since
     * {@code convertForCqlType} accepts both as representations of a UDT and a record field is the form
     * {@code PutCQLRecord} actually produces.
     */
    @Test
    @DisplayName("A UDT supplied as a nested Record with a null field converts too")
    public void testUdtSuppliedAsRecordWithNullFieldIsConvertible() {
        final UserDefinedType addressType = new UserDefinedTypeBuilder("ks", "addr")
                .withField("street", DataTypes.TEXT)
                .withField("state", DataTypes.TEXT)
                .build();

        final RecordSchema nested = schemaOf(
                new RecordField("street", RecordFieldType.STRING.getDataType()),
                new RecordField("state", RecordFieldType.STRING.getDataType()));
        final Map<String, Object> values = new HashMap<>();
        values.put("street", "1 Main St");
        values.put("state", null);

        final Object converted = convertForCqlType(new MapRecord(nested, values), addressType);

        assertTrue(converted instanceof UdtValue, () -> "expected a UdtValue, got " + converted);
        assertTrue(((UdtValue) converted).isNull(CqlIdentifier.fromInternal("state")));
    }

    /**
     * {@code convertForCqlType} is private and has no package-visible caller that avoids a live session, so it
     * is reached reflectively rather than by widening the production API purely for a test. Any exception it
     * throws is unwrapped so the assertion failure names the real cause.
     */
    private Object convertForCqlType(final Object value, final UserDefinedType cqlType) {
        try {
            final Method method = CassandraCQLExecutionService.class
                    .getDeclaredMethod("convertForCqlType", Object.class,
                            com.datastax.oss.driver.api.core.type.DataType.class);
            method.setAccessible(true);
            return method.invoke(service, value, cqlType);
        } catch (final InvocationTargetException e) {
            final Throwable cause = e.getCause();
            return fail("convertForCqlType threw " + cause.getClass().getName() + ": " + cause.getMessage(), cause);
        } catch (final ReflectiveOperationException e) {
            return fail("could not invoke convertForCqlType", e);
        }
    }

    // ------------------------------------------------------------------ override field-name matching

    /**
     * The override lookup's field-name match must be case-insensitive, via {@code CqlIdentifier} normalization:
     * a dynamic property name naturally carries whatever case it was typed with (e.g. {@code ks.tbl.MyField}),
     * while the column it targets is lowercase, unquoted CQL (e.g. {@code myfield}).
     */
    @Test
    @DisplayName("A primary key override declared with a mixed-case field name matches the lowercase column")
    public void testOverrideMatchIsCaseInsensitiveOnFieldName() {
        final RecordPath path = RecordPath.compile("/source");
        final Map<PrimaryKeyIdentifier, RecordPath> overrides =
                Map.of(new PrimaryKeyIdentifier("ks", "t", "MyField"), path);

        final RecordPath matched = getRecordPathOverride(new QualifiedTableName("ks", "t"), "myfield", overrides);

        assertNotNull(matched, "expected the mixed-case override to match the lowercase column name");
        assertEquals(path, matched);
    }

    /**
     * {@code getRecordPathOverride} is private and touches no session, so it is reached reflectively rather
     * than by widening production visibility purely for a test.
     */
    private RecordPath getRecordPathOverride(final QualifiedTableName tableName, final String fieldName,
                                             final Map<PrimaryKeyIdentifier, RecordPath> overrides) {
        try {
            final Method method = CassandraCQLExecutionService.class.getDeclaredMethod(
                    "getRecordPathOverride", QualifiedTableName.class, String.class, Map.class);
            method.setAccessible(true);
            return (RecordPath) method.invoke(service, tableName, fieldName, overrides);
        } catch (final InvocationTargetException e) {
            final Throwable cause = e.getCause();
            return fail("getRecordPathOverride threw " + cause.getClass().getName() + ": " + cause.getMessage(), cause);
        } catch (final ReflectiveOperationException e) {
            return fail("could not invoke getRecordPathOverride", e);
        }
    }
}
