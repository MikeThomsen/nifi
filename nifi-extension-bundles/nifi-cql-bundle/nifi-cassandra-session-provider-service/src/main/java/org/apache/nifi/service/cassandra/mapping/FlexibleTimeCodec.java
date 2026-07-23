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
import java.sql.Time;
import java.time.LocalTime;
import java.util.Optional;

/**
 * Bridges the driver's default CQL {@code time} codec, which only binds an exact {@link LocalTime}, with any
 * value {@code DataTypeUtils} still considers time-compatible - most notably a {@link String}, which is what
 * a RecordPath {@code format()} primary key override produces. The Java type is declared as {@link Object}
 * rather than {@code LocalTime} for the same registration reason documented on {@code FlexibleTinyIntCodec}.
 * <p>
 * A {@link String} input is parsed using the fixed {@code HH:mm:ss} pattern, since this codec - unlike a
 * Record Reader - has no per-call configured Time Format to draw on. Every other supported input type
 * ({@code LocalTime} itself, {@code java.util.Date}/{@code java.sql.Time}, or an epoch-millis {@link Number})
 * ignores the pattern entirely.
 */
public class FlexibleTimeCodec implements TypeCodec<Object> {
    private static final String TIME_PATTERN = "HH:mm:ss";

    private final TypeCodec<LocalTime> timeCodec;

    public FlexibleTimeCodec() {
        this.timeCodec = TypeCodecs.TIME;
    }

    @Override
    public GenericType<Object> getJavaType() {
        return GenericType.of(Object.class);
    }

    @Override
    public DataType getCqlType() {
        return timeCodec.getCqlType(); // maps to CQL `time`
    }

    @Override
    public ByteBuffer encode(Object value, ProtocolVersion protocolVersion) {
        return value == null ? null : timeCodec.encode(toLocalTime(value), protocolVersion);
    }

    @Override
    public Object decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return timeCodec.decode(bytes, protocolVersion);
    }

    @Override
    public String format(Object value) {
        return value == null ? "NULL" : timeCodec.format(toLocalTime(value));
    }

    @Override
    public Object parse(String value) {
        return timeCodec.parse(value);
    }

    private static LocalTime toLocalTime(final Object value) {
        final Object converted = DataTypeUtils.convertType(value, RecordFieldType.TIME.getDataType(),
                Optional.empty(), Optional.of(TIME_PATTERN), Optional.empty(), "value");
        return ((Time) converted).toLocalTime();
    }
}
