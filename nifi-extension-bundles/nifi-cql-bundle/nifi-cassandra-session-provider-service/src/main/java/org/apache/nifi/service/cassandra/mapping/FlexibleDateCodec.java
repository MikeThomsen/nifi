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

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.time.LocalDate;
import java.util.Optional;

/**
 * Bridges the driver's default CQL {@code date} codec, which only binds an exact {@link LocalDate}, with
 * any value {@code DataTypeUtils} still considers date-compatible - most notably a {@link String}, which is
 * what a RecordPath {@code format()} primary key override produces. The Java type is declared as
 * {@link Object} rather than {@code LocalDate} for the same registration reason documented on
 * {@code FlexibleTinyIntCodec}.
 * <p>
 * A {@link String} input is parsed using the fixed {@code yyyy-MM-dd} pattern (ISO local date, matching CQL's
 * own {@code date} literal format and the pattern a {@code format(/path, 'yyyy-MM-dd')} override would
 * naturally use), since this codec - unlike a Record Reader - has no per-call configured Date Format to draw
 * on. Every other supported input type ({@code LocalDate} itself, {@code java.util.Date}/{@code java.sql.Date},
 * or an epoch-millis {@link Number}) ignores the pattern entirely.
 */
public class FlexibleDateCodec implements TypeCodec<Object> {
    private static final String DATE_PATTERN = "yyyy-MM-dd";

    private final TypeCodec<LocalDate> dateCodec;

    public FlexibleDateCodec() {
        this.dateCodec = TypeCodecs.DATE;
    }

    @Override
    public GenericType<Object> getJavaType() {
        return GenericType.of(Object.class);
    }

    @Override
    public DataType getCqlType() {
        return dateCodec.getCqlType(); // maps to CQL `date`
    }

    @Override
    public ByteBuffer encode(Object value, ProtocolVersion protocolVersion) {
        return value == null ? null : dateCodec.encode(toLocalDate(value), protocolVersion);
    }

    @Override
    public Object decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return dateCodec.decode(bytes, protocolVersion);
    }

    @Override
    public String format(Object value) {
        return value == null ? "NULL" : dateCodec.format(toLocalDate(value));
    }

    @Override
    public Object parse(String value) {
        return dateCodec.parse(value);
    }

    private static LocalDate toLocalDate(final Object value) {
        final Object converted = DataTypeUtils.convertType(value, RecordFieldType.DATE.getDataType(),
                Optional.of(DATE_PATTERN), Optional.empty(), Optional.empty(), "value");
        return ((Date) converted).toLocalDate();
    }
}
