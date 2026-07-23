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

package org.apache.nifi.service.cassandra.mapping;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.List;
import java.util.Map;

import static com.datastax.oss.driver.api.core.type.DataTypes.ASCII;

/**
 * Recursively converts a Cassandra CQL {@link DataType} into a nullable-union Avro {@link Schema}, including
 * support for User Defined Types (UDTs) - the one CQL construct {@code CassandraCQLExecutionService} had no
 * way to represent for reads, since Avro has no native concept of a Cassandra UDT.
 *
 * <p>A UDT becomes a nested named Avro record whose fields are converted the same way, so a UDT nested inside
 * another UDT, or a list/set/map holding UDT values, is handled without any special-casing at the call site -
 * the same recursion that resolves a top-level column's type also resolves a UDT field's type.
 */
public final class CassandraUdtSchemaMapper {

    private CassandraUdtSchemaMapper() {
    }

    /**
     * Converts the given CQL type into a nullable Avro union schema.
     *
     * @param cqlType the CQL type to convert
     * @param udtSchemaCache UDT schemas already built during this conversion, keyed by "keyspace.typeName".
     *                        Avro rejects two distinct schema objects that declare the same record name within
     *                        one document, so a UDT referenced by more than one column, or more than one field
     *                        across nested UDTs, must reuse the same generated {@link Schema} instance. Callers
     *                        should pass a fresh, empty map per top-level {@code createSchema} call.
     */
    public static Schema toAvroSchema(final DataType cqlType, final Map<String, Schema> udtSchemaCache) {
        if (cqlType instanceof UserDefinedType udtType) {
            return nullableUnion(toUdtRecordSchema(udtType, udtSchemaCache));
        }

        if (cqlType instanceof ListType listType) {
            return nullableUnion(SchemaBuilder.array().items(toAvroSchema(listType.getElementType(), udtSchemaCache)));
        }

        if (cqlType instanceof SetType setType) {
            return nullableUnion(SchemaBuilder.array().items(toAvroSchema(setType.getElementType(), udtSchemaCache)));
        }

        if (cqlType instanceof MapType mapType) {
            return nullableUnion(SchemaBuilder.map().values(toAvroSchema(mapType.getValueType(), udtSchemaCache)));
        }

        return nullableUnion(toPrimitiveAvroSchema(cqlType));
    }

    private static Schema toUdtRecordSchema(final UserDefinedType udtType, final Map<String, Schema> udtSchemaCache) {
        final String namespace = udtType.getKeyspace() == null ? "cassandra" : udtType.getKeyspace().asInternal();
        final String schemaKey = namespace + "." + udtType.getName().asInternal();

        final Schema cached = udtSchemaCache.get(schemaKey);
        if (cached != null) {
            return cached;
        }

        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record(udtType.getName().asInternal())
                .namespace(namespace)
                .fields();

        final List<CqlIdentifier> fieldNames = udtType.getFieldNames();
        final List<DataType> fieldTypes = udtType.getFieldTypes();

        for (int i = 0; i < fieldNames.size(); i++) {
            fields = fields.name(fieldNames.get(i).asInternal()).type(toAvroSchema(fieldTypes.get(i), udtSchemaCache)).noDefault();
        }

        final Schema schema = fields.endRecord();
        udtSchemaCache.put(schemaKey, schema);
        return schema;
    }

    private static Schema nullableUnion(final Schema schema) {
        return SchemaBuilder.builder().unionOf().nullBuilder().endNull().and().type(schema).endUnion();
    }

    private static Schema toPrimitiveAvroSchema(final DataType cqlType) {
        final SchemaBuilder.TypeBuilder<Schema> typeBuilder = SchemaBuilder.builder();

        if (cqlType.equals(ASCII)
                || cqlType.equals(DataTypes.TEXT)
                // Nonstandard types represented as a string
                || cqlType.equals(DataTypes.TIMESTAMP)
                || cqlType.equals(DataTypes.TIMEUUID)
                || cqlType.equals(DataTypes.UUID)
                || cqlType.equals(DataTypes.INET)
                || cqlType.equals(DataTypes.VARINT)
                || cqlType.equals(DataTypes.DATE)
                || cqlType.equals(DataTypes.TIME)
                || cqlType.equals(DataTypes.DECIMAL)) {
            return typeBuilder.stringType();
        }

        if (cqlType.equals(DataTypes.BOOLEAN)) {
            return typeBuilder.booleanType();
        }

        if (cqlType.equals(DataTypes.INT)
                // Avro has no byte/short primitive, so these widen to Avro's int
                || cqlType.equals(DataTypes.TINYINT)
                || cqlType.equals(DataTypes.SMALLINT)) {
            return typeBuilder.intType();
        }

        if (cqlType.equals(DataTypes.BIGINT) || cqlType.equals(DataTypes.COUNTER)) {
            return typeBuilder.longType();
        }

        if (cqlType.equals(DataTypes.FLOAT)) {
            return typeBuilder.floatType();
        }

        if (cqlType.equals(DataTypes.DOUBLE)) {
            return typeBuilder.doubleType();
        }

        if (cqlType.equals(DataTypes.BLOB)) {
            return typeBuilder.bytesType();
        }

        throw new IllegalArgumentException("Unsupported Cassandra data type " + cqlType + " cannot be converted to an Avro type");
    }
}
