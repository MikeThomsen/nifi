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
package org.apache.nifi.processors.standard;

import com.google.common.base.Joiner;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathPropertyNameValidator;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.apache.commons.codec.binary.StringUtils.getBytesUtf8;

@EventDriven
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@SystemResourceConsideration(resource = SystemResource.MEMORY,
    description = "The HashSet filter type will grow memory space proportionate to the number of unique records processed. " +
        "The BloomFilter type will use constant memory regardless of the number of records processed.")
@Tags({"text", "record", "update", "change", "replace", "modify", "distinct", "unique",
    "filter", "hash", "dupe", "duplicate", "dedupe"})
@CapabilityDescription("This processor attempts to deduplicate a record set in memory using either a hashset or a bloom filter. " +
        "It operates on a per-file basis rather than across an entire data set that spans multiple files.")
@WritesAttribute(attribute = "record.count", description = "The number of records processed.")
@DynamicProperty(
    name = "RecordPath",
    value = "An expression language statement used to determine how the RecordPath is resolved. " +
            "The following variables are availble: ${field.name}, ${field.value}, ${field.type}",
    description = "The name of each user-defined property must be a valid RecordPath.")
@SeeAlso(classNames = {
    "org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService",
    "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer",
    "org.apache.nifi.processors.standard.DetectDuplicate"
})
public class InMemoryDeduplicateRecordSet extends AbstractProcessor {

    private static final String FIELD_NAME = "field.name";
    private static final String FIELD_VALUE = "field.value";
    private static final String FIELD_TYPE = "field.type";

    private volatile RecordPathCache recordPathCache;
    private volatile List<String> recordPaths;

    // VALUES

    static final AllowableValue NONE_ALGORITHM_VALUE = new AllowableValue("none", "None",
            "Do not use a hashing algorithm. The value of resolved RecordPaths will be combined with tildes (~) to form the unique record key. " +
                    "This may use significantly more storage depending on the size and shape or your data.");
    static final AllowableValue MD5_ALGORITHM_VALUE = new AllowableValue(MessageDigestAlgorithms.MD5, "MD5",
            "The MD5 message-digest algorithm.");
    static final AllowableValue SHA1_ALGORITHM_VALUE = new AllowableValue(MessageDigestAlgorithms.SHA_1, "SHA-1",
            "The SHA-1 cryptographic hash algorithm.");
    static final AllowableValue SHA256_ALGORITHM_VALUE = new AllowableValue(MessageDigestAlgorithms.SHA3_256, "SHA-256",
            "The SHA-256 cryptographic hash algorithm.");
    static final AllowableValue SHA512_ALGORITHM_VALUE = new AllowableValue(MessageDigestAlgorithms.SHA3_512, "SHA-512",
            "The SHA-512 cryptographic hash algorithm.");

    static final AllowableValue HASH_SET_VALUE = new AllowableValue("hash-set", "HashSet",
            "Exactly matches records seen before with 100% accuracy at the expense of more storage usage. " +
                    "Stores the filter data in a single cache entry in the distributed cache, and is loaded entirely into memory during duplicate detection. " +
                    "This filter is preferred for small to medium data sets and offers high performance  loaded into memory when this processor is running.");
    static final AllowableValue BLOOM_FILTER_VALUE = new AllowableValue("bloom-filter", "BloomFilter",
            "Space-efficient data structure ideal for large data sets using probability to determine if a record was seen previously. " +
                    "False positive matches are possible, but false negatives are not – in other words, a query returns either \"possibly in the set\" or \"definitely not in the set\". " +
                    "You should use this option if the FlowFile content is large and you can tolerate some duplication in the data. Uses constant storage space regardless of the record set size.");

    // PROPERTIES

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor INCLUDE_ZERO_RECORD_FLOWFILES = new PropertyDescriptor.Builder()
            .name("include-zero-record-flowfiles")
            .displayName("Include Zero Record FlowFiles")
            .description("When converting an incoming FlowFile, if the conversion results in no data, "
                    + "this property specifies whether or not a FlowFile will be sent to the corresponding relationship")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_HASHING_ALGORITHM = new PropertyDescriptor.Builder()
            .name("record-hashing-algorithm")
            .displayName("Record Hashing Algorithm")
            .description("The algorithm used to hash the combined set of resolved RecordPath values for cache storage.")
            .allowableValues(
                    NONE_ALGORITHM_VALUE,
                    MD5_ALGORITHM_VALUE,
                    SHA1_ALGORITHM_VALUE,
                    SHA256_ALGORITHM_VALUE,
                    SHA512_ALGORITHM_VALUE
            )
            .defaultValue(SHA1_ALGORITHM_VALUE.getValue())
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    static final PropertyDescriptor FILTER_TYPE = new PropertyDescriptor.Builder()
            .name("filter-type")
            .displayName("Filter Type")
            .description("The filter used to determine whether a record has been seen before based on the matching RecordPath criteria.")
            .allowableValues(
                    HASH_SET_VALUE,
                    BLOOM_FILTER_VALUE
            )
            .defaultValue(HASH_SET_VALUE.getValue())
            .required(true)
            .build();

    static final PropertyDescriptor FILTER_CAPACITY_HINT = new PropertyDescriptor.Builder()
            .name("filter-capacity-hint")
            .displayName("Filter Capacity Hint")
            .description("An estimation of the total number of unique records to be processed. " +
                    "The more accurate this number is will lead to fewer false negatives on a BloomFilter.")
            .defaultValue("25000")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor BLOOM_FILTER_FPP = new PropertyDescriptor.Builder()
            .name("bloom-filter-certainty")
            .displayName("Bloom Filter Certainty")
            .description("The desired false positive probability when using the BloomFilter type. " +
                    "Using a value of .05 for example, guarantees a five-percent probability that the result is a false positive. " +
                    "The closer to 1 this value is set, the more precise the result at the expense of more storage space utilization.")
            .defaultValue("0.10")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .required(false)
            .build();


    // RELATIONSHIPS

    static final Relationship REL_DUPLICATE = new Relationship.Builder()
            .name("duplicate")
            .description("Records detected as duplicates in the FlowFile content will be routed to this relationship")
            .build();

    static final Relationship REL_NON_DUPLICATE = new Relationship.Builder()
            .name("non-duplicate")
            .description("If the record was not found in the cache, it will be routed to this relationship")
            .build();

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original input FlowFile is sent to this relationship unless there is a fatal error in the processing.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If unable to communicate with the cache, the FlowFile will be penalized and routed to this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(RECORD_READER);
        descriptors.add(RECORD_WRITER);
        descriptors.add(INCLUDE_ZERO_RECORD_FLOWFILES);
        descriptors.add(RECORD_HASHING_ALGORITHM);
        descriptors.add(FILTER_TYPE);
        descriptors.add(FILTER_CAPACITY_HINT);
        descriptors.add(BLOOM_FILTER_FPP);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_DUPLICATE);
        relationships.add(REL_NON_DUPLICATE);
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Specifies a value to use from the record that matches the RecordPath: '" +
                        propertyDescriptorName + "' which is used together with other specified " +
                        "record path values to determine the uniqueness of a record. " +
                        "Expression Language may reference variables 'field.name', 'field.type', and 'field.value' " +
                        "to access information about the field and the value of the field being evaluated.")
                .required(false)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .addValidator(new RecordPathPropertyNameValidator())
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        RecordPathValidator recordPathValidator = new RecordPathValidator();
        final List<ValidationResult> validationResults = validationContext.getProperties().keySet().stream()
                .filter(PropertyDescriptor::isDynamic)
                .map(property -> recordPathValidator.validate(
                        "User-defined Properties",
                        property.getName(),
                        validationContext
                )).collect(Collectors.toList());

        if (validationContext.getProperty(BLOOM_FILTER_FPP).isSet()) {
            final double falsePositiveProbability = validationContext.getProperty(BLOOM_FILTER_FPP).asDouble();
            if (falsePositiveProbability < 0 || falsePositiveProbability > 1) {
                validationResults.add(
                        new ValidationResult.Builder()
                                .subject(BLOOM_FILTER_FPP.getName() + " out of range.")
                                .input(String.valueOf(falsePositiveProbability))
                                .explanation("Valid values are 0.0 - 1.0 inclusive")
                                .valid(false).build());
            }
        }

        return validationResults;
    }

    @OnScheduled
    public void compileRecordPaths(final ProcessContext context) {
        final List<String> recordPaths = new ArrayList<>();

        recordPaths.addAll(context.getProperties().keySet().stream()
                .filter(PropertyDescriptor::isDynamic)
                .map(PropertyDescriptor::getName)
                .collect(toList()));

        recordPathCache = new RecordPathCache(recordPaths.size());
        this.recordPaths = recordPaths;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        FlowFile nonDuplicatesFlowFile = session.create(flowFile);
        FlowFile duplicatesFlowFile = session.create(flowFile);

        try {
            final long now = System.currentTimeMillis();

            final int filterCapacity = context.getProperty(FILTER_CAPACITY_HINT).asInteger();
            Serializable serializableFilter = context.getProperty(FILTER_TYPE).getValue()
                    .equals(context.getProperty(HASH_SET_VALUE.getValue()))
                    ? new HashSet<String>(filterCapacity)
                    : BloomFilter.create(
                    Funnels.stringFunnel(Charset.defaultCharset()),
                    filterCapacity,
                    context.getProperty(BLOOM_FILTER_FPP).asDouble());

            final FilterWrapper filter = FilterWrapper.create(serializableFilter);

            final String recordHashingAlgorithm = context.getProperty(RECORD_HASHING_ALGORITHM).getValue();
            final MessageDigest messageDigest = recordHashingAlgorithm.equals(NONE_ALGORITHM_VALUE.getValue())
                    ? null
                    : DigestUtils.getDigest(recordHashingAlgorithm);
            final Boolean matchWholeRecord = context.getProperties().keySet().stream().noneMatch(p -> p.isDynamic());

            final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
            final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

            final InputStream inputStream = session.read(flowFile);

            final RecordReader reader = readerFactory.createRecordReader(flowFile, inputStream, logger);

            final RecordSchema writeSchema = writerFactory.getSchema(flowFile.getAttributes(), reader.getSchema());

            final OutputStream nonDupeStream = session.write(nonDuplicatesFlowFile);
            final OutputStream dupeStream = session.write(duplicatesFlowFile);

            final RecordSetWriter nonDuplicatesWriter = writerFactory.createWriter(getLogger(), writeSchema, nonDupeStream);
            final RecordSetWriter duplicatesWriter = writerFactory.createWriter(getLogger(), writeSchema, dupeStream);

            nonDuplicatesWriter.beginRecordSet();
            duplicatesWriter.beginRecordSet();
            Record record;

            while ((record = reader.nextRecord()) != null) {
                String recordValue;

                if (matchWholeRecord) {
                    recordValue = Joiner.on('~').join(record.getValues());
                } else {
                    final List<String> fieldValues = new ArrayList<>();
                    for (final String recordPathText : recordPaths) {
                        final PropertyValue recordPathPropertyValue = context.getProperty(recordPathText);
                        final RecordPath recordPath = recordPathCache.getCompiled(recordPathText);
                        final RecordPathResult result = recordPath.evaluate(record);
                        final List<FieldValue> selectedFields = result.getSelectedFields().collect(Collectors.toList());

                        if(recordPathPropertyValue.isExpressionLanguagePresent()) {
                            final Map<String, String> fieldVariables = new HashMap<>();
                            selectedFields.forEach(fieldVal -> {
                                fieldVariables.clear();
                                fieldVariables.put(FIELD_NAME, fieldVal.getField().getFieldName());
                                fieldVariables.put(FIELD_VALUE, DataTypeUtils.toString(fieldVal.getValue(), (String) null));
                                fieldVariables.put(FIELD_TYPE, fieldVal.getField().getDataType().getFieldType().name());

                                fieldValues.add(recordPathPropertyValue.evaluateAttributeExpressions(flowFile, fieldVariables).getValue());
                            });
                        } else {
                            fieldValues.add(recordPathPropertyValue.evaluateAttributeExpressions(flowFile).getValue());
                        }

                        fieldValues.addAll(selectedFields.stream()
                                        .map(f -> recordPathPropertyValue.evaluateAttributeExpressions(flowFile).getValue())
                                        .collect(toList())
                        );
                    }
                    recordValue = Joiner.on('~').join(fieldValues);
                }

                final String recordHash = messageDigest != null
                        ? Hex.encodeHexString(messageDigest.digest(getBytesUtf8(recordValue)))
                        : recordValue;

                if(filter.contains(recordHash)) {
                    duplicatesWriter.write(record);
                } else {
                    nonDuplicatesWriter.write(record);
                }

                filter.put(recordHash);
            }

            reader.close();
            inputStream.close();

            final boolean includeZeroRecordFlowFiles = context.getProperty(INCLUDE_ZERO_RECORD_FLOWFILES).isSet()
                    ? context.getProperty(INCLUDE_ZERO_RECORD_FLOWFILES).asBoolean()
                    : true;

            // Route Non-Duplicates FlowFile
            final WriteResult nonDuplicatesWriteResult = nonDuplicatesWriter.finishRecordSet();
            nonDuplicatesWriter.close();
            nonDupeStream.close();
            Map<String, String> attributes = new HashMap<>();
            attributes.putAll(nonDuplicatesWriteResult.getAttributes());
            attributes.put("record.count", String.valueOf(nonDuplicatesWriteResult.getRecordCount()));
            attributes.put(CoreAttributes.MIME_TYPE.key(), nonDuplicatesWriter.getMimeType());
            nonDuplicatesFlowFile = session.putAllAttributes(nonDuplicatesFlowFile, attributes);
            logger.info("Successfully found {} unique records for {}", new Object[] {nonDuplicatesWriteResult.getRecordCount(), nonDuplicatesFlowFile});

            // Route Duplicates FlowFile
            final WriteResult duplicatesWriteResult = duplicatesWriter.finishRecordSet();
            duplicatesWriter.close();
            dupeStream.close();
            attributes.clear();
            attributes.putAll(duplicatesWriteResult.getAttributes());
            attributes.put("record.count", String.valueOf(duplicatesWriteResult.getRecordCount()));
            attributes.put(CoreAttributes.MIME_TYPE.key(), duplicatesWriter.getMimeType());
            duplicatesFlowFile = session.putAllAttributes(duplicatesFlowFile, attributes);
            logger.info("Successfully found {} duplicate records for {}", new Object[] {duplicatesWriteResult.getRecordCount(), nonDuplicatesFlowFile});



            session.adjustCounter("Records Processed",
                    nonDuplicatesWriteResult.getRecordCount() + duplicatesWriteResult.getRecordCount(), false);

            if(!includeZeroRecordFlowFiles && duplicatesWriteResult.getRecordCount() == 0) {
                session.remove(duplicatesFlowFile);
            } else {
                session.transfer(duplicatesFlowFile, REL_DUPLICATE);
            }

            if(!includeZeroRecordFlowFiles && nonDuplicatesWriteResult.getRecordCount() == 0) {
                session.remove(nonDuplicatesFlowFile);
            } else {
                session.transfer(nonDuplicatesFlowFile, REL_NON_DUPLICATE);
            }

            session.transfer(flowFile, REL_ORIGINAL);

        } catch (final Exception e) {
            logger.error("Failed in detecting duplicate records.", e);
            session.remove(duplicatesFlowFile);
            session.remove(nonDuplicatesFlowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
    }

    private abstract static class FilterWrapper {
        public static FilterWrapper create(Object filter) {
            if(filter instanceof HashSet) {
                return new HashSetFilterWrapper((HashSet<String>) filter);
            } else {
                return new BloomFilterWrapper((BloomFilter<String>) filter);
            }
        }
        public abstract boolean contains(String value);
        public abstract void put(String value);
    }

    private static class HashSetFilterWrapper extends FilterWrapper {

        private final HashSet<String> filter;

        public HashSetFilterWrapper(HashSet<String> filter) {
            this.filter = filter;
        }

        @Override
        public boolean contains(String value) {
            return filter.contains(value);
        }

        @Override
        public void put(String value) {
            filter.add(value);
        }
    }

    private static class BloomFilterWrapper extends FilterWrapper {

        private final BloomFilter<String> filter;

        public BloomFilterWrapper(BloomFilter<String> filter) {
            this.filter = filter;
        }

        @Override
        public boolean contains(String value) {
            return filter.mightContain(value);
        }

        @Override
        public void put(String value) {
            filter.put(value);
        }
    }

    private static class CacheValue implements Serializable {

        private final Serializable filter;
        private final long entryTimeMS;

        public CacheValue(Serializable filter, long entryTimeMS) {
            this.filter = filter;
            this.entryTimeMS = entryTimeMS;
        }

        public Serializable getFilter() {
            return filter;
        }

        public long getEntryTimeMS() {
            return entryTimeMS;
        }
    }
}
