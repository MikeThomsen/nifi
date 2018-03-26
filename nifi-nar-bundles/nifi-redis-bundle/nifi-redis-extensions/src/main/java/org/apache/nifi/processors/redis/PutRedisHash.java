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

package org.apache.nifi.processors.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.redis.RedisConnectionPool;
import org.apache.nifi.util.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class PutRedisHash extends AbstractRedisProcessor {

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("If the operations succeed, the flowfile goes to this relationship.")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("If the operations fail, the flowfile goes to this relationship.")
        .build();

    static final Set<Relationship> relationships;
    static final List<PropertyDescriptor> propertyDescriptors;

    static final PropertyDescriptor REDIS_CONNECTION_POOL = new PropertyDescriptor.Builder()
        .name("redis-connection-pool")
        .displayName("Redis Connection Pool")
        .identifiesControllerService(RedisConnectionPool.class)
        .required(true)
        .build();

    static final AllowableValue CPS_ERROR = new AllowableValue("error", "Raise an error.");
    static final AllowableValue CPS_WARN  = new AllowableValue("warn", "Warn and ignore.");
    static final AllowableValue CPS_JSON  = new AllowableValue("json", "Convert to JSON and store JSON.");

    static final PropertyDescriptor COMPLEX_PROPERTY_STRATEGY = new PropertyDescriptor.Builder()
        .name("redis-complex-property-strategy")
        .displayName("Complex Property Strategy")
        .description("The strategy that is used to handle key/value pairs where the value is a list or a hash, not a simple value")
        .allowableValues(CPS_JSON, CPS_ERROR, CPS_WARN)
        .defaultValue(CPS_JSON.getValue())
        .required(true)
        .build();

    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
        .name("redis-charset")
        .displayName("Charset")
        .description("The charset used to read the JSON payload.")
        .defaultValue("UTF-8")
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .required(true)
        .build();

    static final PropertyDescriptor NAME_ATTRIBUTE = new PropertyDescriptor.Builder()
        .name("redis-name-attribute")
        .displayName("Name Attribute")
        .description("The attribute on the incoming flowfile that will be used for the Redis HashMap's name")
        .required(true)
        .defaultValue("redis.hashmap.name")
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();

    static {
        List<Relationship> _rels = new ArrayList<>();
        _rels.add(REL_SUCCESS);
        _rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(new HashSet<>(_rels));

        List<PropertyDescriptor> _props = new ArrayList<>();
        _props.add(REDIS_CONNECTION_POOL);
        _props.add(COMPLEX_PROPERTY_STRATEGY);
        _props.add(NAME_ATTRIBUTE);
        _props.add(CHARSET);

        propertyDescriptors = Collections.unmodifiableList(_props);
    }

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        final String nameAttribute = context.getProperty(NAME_ATTRIBUTE).evaluateAttributeExpressions(input).getValue();
        final String attrValue     = input.getAttribute(nameAttribute);
        if (StringUtils.isBlank(attrValue)) {
            getLogger().error(String.format("Expected a name attribute of \"%s\", but did not find one.", nameAttribute));
            session.transfer(input, REL_FAILURE);
            return;
        }
        final Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());

        String body;
        try {
            body = readBody(input, session, charset);
            if (body.startsWith("[")) {
                throw new Exception("PutRedisHash does not support arrays as the top level type.");
            }
        } catch (Exception e) {
            getLogger().error("Failed to read the flowfile body.", e);
            session.transfer(input, REL_FAILURE);
            return;
        }

        try {
            Map<String, Object> json = mapper.readValue(body, Map.class);
            final Map<byte[], byte[]> converted = new HashMap<>();
            for (Map.Entry<String, Object> entry : json.entrySet()) {
                byte[] key = entry.getKey().getBytes();
                byte[] val = serializeValue(entry.getValue(), charset);
                converted.put(key, val);
            }
            withConnection(redisConnection -> {
                redisConnection.hMSet(attrValue.getBytes(charset), converted);
                return null;
            });
            session.transfer(input, REL_SUCCESS);
        } catch (Exception ex) {
            ex.printStackTrace();
            getLogger().error("Error processing flowfile.", ex);
            session.transfer(input, REL_FAILURE);
            context.yield();
        }
    }

    private byte[] serializeValue(Object o, Charset charset) {
        if (o instanceof String) {
            return ((String) o).getBytes(charset);
        } else if (o instanceof Integer) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt((Integer)o);
            return bb.array();
        } else if (o instanceof Long) {
            ByteBuffer bb = ByteBuffer.allocate(8);
            bb.putLong((Long)o);
            return bb.array();
        } else if (o instanceof Boolean) {
            return o.toString().getBytes(charset);
        } else {
            throw new IllegalArgumentException(String.format("%s is not a valid input.", o.getClass().getName()));
        }
    }

    private String readBody(FlowFile input, ProcessSession session, Charset charset) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        session.exportTo(input, out);
        out.close();

        return new String(out.toByteArray(), charset);
    }
}
