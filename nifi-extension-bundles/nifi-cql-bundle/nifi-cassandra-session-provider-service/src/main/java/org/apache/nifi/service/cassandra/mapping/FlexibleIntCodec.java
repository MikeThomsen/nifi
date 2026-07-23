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
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.nio.ByteBuffer;

/**
 * Bridges the driver's default CQL {@code int} codec, which only binds an exact {@link Integer},
 * with any value {@code DataTypeUtils} still considers int-compatible (for example a numeric
 * String, or a differently-sized {@link Number}). The Java type is declared as {@link Object}
 * rather than {@code Integer} - the built-in codec already claims {@code Integer} exactly, so this
 * codec would be silently rejected as a duplicate at registration time if it claimed the same
 * type. Declaring {@code Object} makes it a fallback the registry only reaches once the built-in
 * codec's exact-type match fails.
 */
public class FlexibleIntCodec implements TypeCodec<Object> {
    private final TypeCodec<Integer> intCodec;

    public FlexibleIntCodec() {
        this.intCodec = TypeCodecs.INT;
    }

    @Override
    public GenericType<Object> getJavaType() {
        return GenericType.of(Object.class);
    }

    @Override
    public DataType getCqlType() {
        return intCodec.getCqlType(); // maps to CQL `int`
    }

    @Override
    public ByteBuffer encode(Object value, ProtocolVersion protocolVersion) {
        return value == null ? null : intCodec.encode(DataTypeUtils.toInteger(value, getCqlType().toString()), protocolVersion);
    }

    @Override
    public Object decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return intCodec.decode(bytes, protocolVersion);
    }

    @Override
    public String format(Object value) {
        return value == null ? "NULL" : intCodec.format(DataTypeUtils.toInteger(value, getCqlType().toString()));
    }

    @Override
    public Object parse(String value) {
        return intCodec.parse(value);
    }
}
