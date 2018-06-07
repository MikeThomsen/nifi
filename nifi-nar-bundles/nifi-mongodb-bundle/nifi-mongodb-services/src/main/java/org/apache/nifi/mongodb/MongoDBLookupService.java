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

package org.apache.nifi.mongodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.JsonInferenceSchemaRegistryService;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StringUtils;
import org.bson.Document;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.schema.access.SchemaAccessUtils.INFER_SCHEMA;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_BRANCH_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_VERSION;

@Tags({"mongo", "mongodb", "lookup", "record"})
@CapabilityDescription(
    "Provides a lookup service based around MongoDB. Each key that is specified \n" +
    "will be added to a query as-is. For example, if you specify the two keys, \n" +
    "user and email, the resulting query will be { \"user\": \"tester\", \"email\": \"tester@test.com\" }.\n" +
    "The query is limited to the first result (findOne in the Mongo documentation). If no \"Lookup Value Field\" is specified " +
    "then the entire MongoDB result document minus the _id field will be returned as a record."
)
public class MongoDBLookupService extends JsonInferenceSchemaRegistryService implements LookupService<Object> {
    public static final PropertyDescriptor CONTROLLER_SERVICE = new PropertyDescriptor.Builder()
        .name("mongo-lookup-client-service")
        .displayName("Client Service")
        .description("A MongoDB controller service to use with this lookup service.")
        .required(true)
        .identifiesControllerService(MongoDBClientService.class)
        .build();

    public static final PropertyDescriptor LOOKUP_VALUE_FIELD = new PropertyDescriptor.Builder()
        .name("mongo-lookup-value-field")
        .displayName("Lookup Value Field")
        .description("The field whose value will be returned when the lookup key(s) match a record. If not specified then the entire " +
                "MongoDB result document minus the _id field will be returned as a record.")
        .addValidator(Validator.VALID)
        .required(false)
        .build();
    public static final PropertyDescriptor PROJECTION = new PropertyDescriptor.Builder()
        .name("mongo-lookup-projection")
        .displayName("Projection")
        .description("Specifies a projection for limiting which fields will be returned.")
        .required(false)
        .build();

    private String lookupValueField;

    @Override
    public Optional<Object> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        Map<String, Object> clean = coordinates.entrySet().stream()
            .filter(e -> !schemaNameProperty.equals(String.format("${%s}", e.getKey())))
            .collect(Collectors.toMap(
                e -> e.getKey(),
                e -> e.getValue()
            ));
        Document query = new Document(clean);

        if (coordinates.size() == 0) {
            throw new LookupFailureException("No keys were configured. Mongo query would return random documents.");
        }

        try {
            Document result = projection != null ? controllerService.findOne(query, projection) : controllerService.findOne(query);

            if(result == null) {
                return Optional.empty();
            } else if (!StringUtils.isEmpty(lookupValueField)) {
                return Optional.ofNullable(result.get(lookupValueField));
            } else {
                RecordSchema schema = loadSchema(coordinates, result);

                return Optional.ofNullable(new MapRecord(schema, result));
            }
        } catch (Exception ex) {
            getLogger().error("Error during lookup {}", new Object[]{ query.toJson() }, ex);
            throw new LookupFailureException(ex);
        }
    }

    private RecordSchema loadSchema(Map<String, Object> coordinates, Document doc) {
        Map<String, String> variables = coordinates.entrySet().stream()
            .collect(Collectors.toMap(
                e -> e.getKey(),
                e -> e.getValue().toString()
            ));
        ObjectMapper mapper = new ObjectMapper();
        try {
            byte[] bytes = mapper.writeValueAsBytes(doc);
            return getSchema(variables, new ByteArrayInputStream(bytes), null);
        } catch (Exception ex) {
            return null;
        }
    }

    private volatile Document projection;
    private MongoDBClientService controllerService;
    private String schemaNameProperty;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.lookupValueField = context.getProperty(LOOKUP_VALUE_FIELD).getValue();
        this.controllerService = context.getProperty(CONTROLLER_SERVICE).asControllerService(MongoDBClientService.class);

        this.schemaNameProperty = context.getProperty(SchemaAccessUtils.SCHEMA_NAME).getValue();

        String configuredProjection = context.getProperty(PROJECTION).isSet()
            ? context.getProperty(PROJECTION).getValue()
            : null;
        if (!StringUtils.isBlank(configuredProjection)) {
            projection = Document.parse(configuredProjection);
        }
    }

    @Override
    public Class<?> getValueType() {
        return Record.class;
    }

    @Override
    public Set<String> getRequiredKeys() {
        return Collections.emptySet();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        AllowableValue[] strategies = new AllowableValue[] {
            SCHEMA_NAME_PROPERTY, SCHEMA_TEXT_PROPERTY, INFER_SCHEMA
        };
        List<PropertyDescriptor> _temp = new ArrayList<>();
        _temp.add(new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(SCHEMA_ACCESS_STRATEGY)
                .allowableValues(strategies)
                .defaultValue(getDefaultSchemaAccessStrategy().getValue())
                .build());

        _temp.add(SCHEMA_REGISTRY);
        _temp.add(SCHEMA_NAME);
        _temp.add(SCHEMA_VERSION);
        _temp.add(SCHEMA_BRANCH_NAME);
        _temp.add(SCHEMA_TEXT);
        _temp.add(CONTROLLER_SERVICE);
        _temp.add(LOOKUP_VALUE_FIELD);
        _temp.add(PROJECTION);

        return Collections.unmodifiableList(_temp);
    }
}
