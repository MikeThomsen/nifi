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
package org.apache.nifi.processors.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.ContainerType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.cassandra.CassandraSessionProviderService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.datastax.oss.driver.api.core.type.DataTypes.*;

/**
 * AbstractCassandraProcessor is a base class for Cassandra processors and contains logic and variables common to most
 * processors integrating with Apache Cassandra.
 */
public abstract class AbstractCassandraProcessor extends AbstractProcessor {

    public static final int DEFAULT_CASSANDRA_PORT = 9042;

    // Common descriptors
    static final PropertyDescriptor CONNECTION_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("cassandra-connection-provider")
            .displayName("Cassandra Connection Provider")
            .description("Specifies the Cassandra connection providing controller service to be used to connect to Cassandra cluster.")
            .required(true)
            .identifiesControllerService(CassandraSessionProviderService.class)
            .build();

    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("Specifies the character set of the record data.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is transferred to this relationship if the operation completed successfully.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is transferred to this relationship if the operation failed.")
            .build();

    static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("A FlowFile is transferred to this relationship if the operation cannot be completed but attempting "
                    + "it again may succeed.")
            .build();

    protected static List<PropertyDescriptor> descriptors = new ArrayList<>();

    static {
        descriptors.add(CONNECTION_PROVIDER_SERVICE);
        descriptors.add(CHARSET);
    }

    protected final AtomicReference<CqlSession> cassandraSession = new AtomicReference<>(null);

    protected static final DefaultCodecRegistry codecRegistry = new DefaultCodecRegistry("nifi");

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        registerAdditionalCodecs();

        CassandraSessionProviderService sessionProvider = context.getProperty(CONNECTION_PROVIDER_SERVICE).asControllerService(CassandraSessionProviderService.class);
        cassandraSession.set(sessionProvider.getCassandraSession());
    }

    protected void registerAdditionalCodecs() {
        // Conversion between a String[] and a list of varchar
        codecRegistry.register(new ObjectArrayCodec<>(
                DataTypes.listOf(TEXT),
                String[].class,
                TypeCodecs.TEXT));
    }

    public void stop(ProcessContext context) {
        // We don't want to close the connection when using 'Cassandra Connection Provider'
        // because each time @OnUnscheduled/@OnShutdown annotated method is triggered on a
        // processor, the connection would be closed which is not ideal for a centralized
        // connection provider controller service
        if (!context.getProperty(CONNECTION_PROVIDER_SERVICE).isSet()) {
            if (cassandraSession.get() != null) {
                cassandraSession.get().close();
                cassandraSession.set(null);
            }
        }
    }


    protected static Object getCassandraObject(Row row, int i, DataType dataType) {
        if (dataType.equals(DataTypes.BLOB)) {
            return row.getByteBuffer(i).array();

        } else if (dataType.equals(DataTypes.VARINT) || dataType.equals(DataTypes.DECIMAL)) {
            // Avro can't handle BigDecimal and BigInteger as numbers - it will throw an
            // AvroRuntimeException such as: "Unknown datum type: java.math.BigDecimal: 38"
            return row.getObject(i).toString();

        } else if (dataType.equals(DataTypes.BOOLEAN)) {
            return row.getBool(i);

        } else if (dataType.equals(DataTypes.INT)) {
            return row.getInt(i);

        } else if (dataType.equals(DataTypes.BIGINT)
                || dataType.equals(DataTypes.COUNTER)) {
            return row.getLong(i);

        } else if (dataType.equals(ASCII)
                || dataType.equals(DataTypes.TEXT)) {
            return row.getString(i);

        } else if (dataType.equals(DataTypes.FLOAT)) {
            return row.getFloat(i);

        } else if (dataType.equals(DataTypes.DOUBLE)) {
            return row.getDouble(i);

        } else if (dataType.equals(DataTypes.TIMESTAMP)) {
            return row.getLocalTime(i);
        } else if (dataType.equals(DataTypes.DATE)) {
            return row.getLocalDate(i);

        } else if (dataType.equals(DataTypes.TIME)) {
            return row.getLocalTime(i);

        } else if (dataType instanceof MapType || dataType instanceof TupleType || dataType instanceof ContainerType) {

            List<DataType> typeArguments = switch(dataType) {
                case MapType type -> Arrays.asList(type.getKeyType(), type.getValueType());
                case TupleType type -> type.getComponentTypes();
                case ContainerType type -> Arrays.asList(type.getElementType());
                default -> throw new IllegalArgumentException("Column[" + i + "] " + dataType
                        + " is a collection but no type arguments were specified!");
            };

            // Get the first type argument, to be used for lists and sets (and the first in a map)
            DataType firstArg = typeArguments.get(0);
            TypeCodec firstCodec = codecRegistry.codecFor(firstArg);
            if (dataType.equals(DataTypes.setOf(firstArg))) {
                return row.getSet(i, firstCodec.getJavaType().getComponentType().getRawType());
            } else if (dataType.equals(DataTypes.listOf(firstArg))) {
                return row.getList(i, firstCodec.getJavaType().getComponentType().getRawType());
            } else {
                // Must be an n-arg collection like map
                DataType secondArg = typeArguments.get(1);
                TypeCodec secondCodec = codecRegistry.codecFor(secondArg);
                if (dataType.equals(DataTypes.mapOf(firstArg, secondArg))) {
                    return row.getMap(i, firstCodec.getJavaType().getRawType(), secondCodec.getJavaType().getRawType());
                }
            }

        } else {
            // The different types that we support are numbers (int, long, double, float),
            // as well as boolean values and Strings. Since Avro doesn't provide
            // timestamp types, we want to convert those to Strings. So we will cast anything other
            // than numbers or booleans to strings by using the toString() method.
            return row.getObject(i).toString();
        }
        return null;
    }

    /**
     * This method will create a schema a union field consisting of null and the specified type.
     *
     * @param dataType The data type of the field
     */
    protected static Schema getUnionFieldType(String dataType) {
        return SchemaBuilder.builder().unionOf().nullBuilder().endNull().and().type(getSchemaForType(dataType)).endUnion();
    }

    /**
     * This method will create an Avro schema for the specified type.
     *
     * @param dataType The data type of the field
     */
    protected static Schema getSchemaForType(final String dataType) {
        final SchemaBuilder.TypeBuilder<Schema> typeBuilder = SchemaBuilder.builder();
        final Schema returnSchema = switch (dataType) {
            case "string" -> typeBuilder.stringType();
            case "boolean" -> typeBuilder.booleanType();
            case "int" -> typeBuilder.intType();
            case "long" -> typeBuilder.longType();
            case "float" -> typeBuilder.floatType();
            case "double" -> typeBuilder.doubleType();
            case "bytes" -> typeBuilder.bytesType();
            default -> throw new IllegalArgumentException("Unknown Avro primitive type: " + dataType);
        };
        return returnSchema;
    }

    protected static String getPrimitiveAvroTypeFromCassandraType(final DataType dataType) {
        // Map types from Cassandra to Avro where possible
        if (dataType.equals(ASCII)
                || dataType.equals(DataTypes.TEXT)
                // Nonstandard types represented by this processor as a string
                || dataType.equals(DataTypes.TIMESTAMP)
                || dataType.equals(DataTypes.TIMEUUID)
                || dataType.equals(DataTypes.UUID)
                || dataType.equals(DataTypes.INET)
                || dataType.equals(DataTypes.VARINT)) {
            return "string";

        } else if (dataType.equals(DataTypes.BOOLEAN)) {
            return "boolean";

        } else if (dataType.equals(DataTypes.INT)) {
            return "int";

        } else if (dataType.equals(DataTypes.BIGINT)
                || dataType.equals(DataTypes.COUNTER)) {
            return "long";

        } else if (dataType.equals(DataTypes.FLOAT)) {
            return "float";

        } else if (dataType.equals(DataTypes.DOUBLE)) {
            return "double";

        } else if (dataType.equals(DataTypes.BLOB)) {
            return "bytes";

        } else {
            throw new IllegalArgumentException("createSchema: Unknown Cassandra data type " + dataType
                    + " cannot be converted to Avro type");
        }
    }

    protected static DataType getPrimitiveDataTypeFromString(String dataTypeName) {
        Set<DataType> primitiveTypes = new HashSet<>(Arrays.asList(
           ASCII, BIGINT, BLOB, BOOLEAN, COUNTER, DECIMAL, DOUBLE, FLOAT, INT, TIMESTAMP, UUID,
                VARINT, TIMEUUID, INET, DATE, TEXT, TIME, SMALLINT, TINYINT, DURATION
        ));
        for (DataType primitiveType : primitiveTypes) {
            if (primitiveType.toString().equals(dataTypeName)) {
                return primitiveType;
            }
        }
        return null;
    }

    /**
     * Gets a list of InetSocketAddress objects that correspond to host:port entries for Cassandra contact points
     *
     * @param contactPointList A comma-separated list of Cassandra contact points (host:port,host2:port2, etc.)
     * @return List of InetSocketAddresses for the Cassandra contact points
     */
    protected List<InetSocketAddress> getContactPoints(String contactPointList) {

        if (contactPointList == null) {
            return null;
        }
        final String[] contactPointStringList = contactPointList.split(",");
        List<InetSocketAddress> contactPoints = new ArrayList<>();

        for (String contactPointEntry : contactPointStringList) {

            String[] addresses = contactPointEntry.split(":");
            final String hostName = addresses[0].trim();
            final int port = (addresses.length > 1) ? Integer.parseInt(addresses[1].trim()) : DEFAULT_CASSANDRA_PORT;

            contactPoints.add(new InetSocketAddress(hostName, port));
        }
        return contactPoints;
    }
}

