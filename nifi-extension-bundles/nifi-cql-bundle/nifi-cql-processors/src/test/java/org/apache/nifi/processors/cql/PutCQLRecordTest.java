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

package org.apache.nifi.processors.cql;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.service.cql.api.CQLExecutionService;
import org.apache.nifi.service.cql.api.CqlBatchType;
import org.apache.nifi.service.cql.api.UpdateMethod;
import org.apache.nifi.service.cql.api.WriteOverrides;
import org.apache.nifi.service.cql.api.exception.QueryFailureException;
import org.apache.nifi.service.cql.api.metadata.PrimaryKeyIdentifier;
import org.apache.nifi.service.cql.api.metadata.QualifiedTableName;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.cql.constants.StatementType.UPDATE_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PutCQLRecordTest {
    private TestRunner runner;
    private CQLExecutionService service;
    private MockRecordParser mockReader;

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(PutCQLRecord.class);
        service = Mockito.mock(CQLExecutionService.class);
        mockReader = new MockRecordParser();
        mockReader.addSchemaField("message", RecordFieldType.STRING);
        mockReader.addSchemaField("sender", RecordFieldType.STRING);

        when(service.getIdentifier()).thenReturn("executionService");
        when(service.getTransitUrl(any(QualifiedTableName.class))).thenReturn("cassandra://message");

        runner.setProperty(PutCQLRecord.CONNECTION_PROVIDER_SERVICE, service.getIdentifier());
        runner.setProperty(PutCQLRecord.TABLE, "message");
        runner.setProperty(PutCQLRecord.RECORD_READER_FACTORY, "reader");

        runner.addControllerService("reader", mockReader);
        runner.enableControllerService(mockReader);

        runner.addControllerService(service.getIdentifier(), service);
        runner.enableControllerService(service);
    }

    @Test
    @DisplayName("Inserting records batches them into the configured batch size and reports success")
    void testInsert() {
        final int recordCount = 1000;
        final int batchCount = 10;

        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        for (int i = 0; i < recordCount; i++) {
            mockReader.addRecord("Hello, world", "test_user");
        }

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        verify(service, times(batchCount))
                .insert(eq(new QualifiedTableName(null, "message")), anyList(), anyMap(), eq(CqlBatchType.LOGGED), eq(WriteOverrides.NONE));
    }

    @DisplayName("Updating records with each update method and batch statement type produces the expected batched calls")
    @ParameterizedTest
    @CsvSource({ "DECREMENT,COUNTER", "INCREMENT,COUNTER", "SET,LOGGED" })
    void testUpdate(UpdateMethod updateMethod, String batchStatementType) {
        runner.setProperty(PutCQLRecord.UPDATE_KEYS, "sender");
        runner.setProperty(PutCQLRecord.STATEMENT_TYPE, UPDATE_TYPE.getValue());
        runner.setProperty(PutCQLRecord.UPDATE_METHOD, updateMethod.name());
        runner.setProperty(PutCQLRecord.BATCH_STATEMENT_TYPE, batchStatementType);

        final int recordCount = 1050;
        final int batchCount = 11;

        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        for (int i = 0; i < recordCount; i++) {
            mockReader.addRecord("Hello, world", "test_user");
        }

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        verify(service, times(batchCount))
                .update(eq(new QualifiedTableName(null, "message")), anyList(), anyMap(), eq(List.of("sender")), eq(updateMethod), eq(CqlBatchType.valueOf(batchStatementType)), eq(WriteOverrides.NONE));
    }

    @Test
    @DisplayName("An update method resolved from a FlowFile attribute is matched case-insensitively")
    void testUpdateMethodFromAttributeIsCaseInsensitive() {
        runner.setProperty(PutCQLRecord.UPDATE_KEYS, "sender");
        runner.setProperty(PutCQLRecord.STATEMENT_TYPE, UPDATE_TYPE.getValue());
        runner.setProperty(PutCQLRecord.UPDATE_METHOD, "USE_ATTR");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("", Map.of(PutCQLRecord.UPDATE_METHOD_ATTRIBUTE, "set"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        verify(service, times(1))
                .update(eq(new QualifiedTableName(null, "message")), anyList(), anyMap(), eq(List.of("sender")), eq(UpdateMethod.SET), eq(CqlBatchType.LOGGED), eq(WriteOverrides.NONE));
    }

    @Test
    @DisplayName("Setting Time To Live on the processor overrides the connection service's default for inserts")
    void testTtlOverrideAppliesToInsert() {
        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");
        runner.setProperty(PutCQLRecord.TTL, "1 hour");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        verify(service, times(1))
                .insert(eq(new QualifiedTableName(null, "message")), anyList(), anyMap(), eq(CqlBatchType.LOGGED), eq(new WriteOverrides(Duration.ofHours(1), null)));
    }

    @Test
    @DisplayName("Setting Time To Live on the processor overrides the connection service's default for SET updates")
    void testTtlOverrideAppliesToUpdate() {
        runner.setProperty(PutCQLRecord.UPDATE_KEYS, "sender");
        runner.setProperty(PutCQLRecord.STATEMENT_TYPE, UPDATE_TYPE.getValue());
        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");
        runner.setProperty(PutCQLRecord.TTL, "30 min");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        verify(service, times(1))
                .update(eq(new QualifiedTableName(null, "message")), anyList(), anyMap(), eq(List.of("sender")), eq(UpdateMethod.SET), eq(CqlBatchType.LOGGED), eq(new WriteOverrides(Duration.ofMinutes(30), null)));
    }

    @Test
    @DisplayName("Setting Timestamp Field on the processor threads the field name through as a write override")
    void testTimestampFieldOverrideAppliesToInsert() {
        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");
        runner.setProperty(PutCQLRecord.TIMESTAMP_FIELD, "sender");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        verify(service, times(1))
                .insert(eq(new QualifiedTableName(null, "message")), anyList(), anyMap(), eq(CqlBatchType.LOGGED), eq(new WriteOverrides(null, "sender")));
    }

    @Test
    @DisplayName("Setting Timestamp Field to a name absent from the record schema routes to failure without writing anything")
    void testTimestampFieldNotInSchemaRoutesToFailure() {
        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");
        runner.setProperty(PutCQLRecord.TIMESTAMP_FIELD, "does_not_exist");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_FAILURE, 1);

        verify(service, times(0)).insert(any(QualifiedTableName.class), anyList(), anyMap(), any(CqlBatchType.class), any(WriteOverrides.class));
    }

    @Test
    @DisplayName("Configuring INSERT with a COUNTER batch statement type fails validation up front")
    void testInsertWithCounterBatchTypeIsInvalid() {
        // Statement Type defaults to INSERT; Cassandra/ScyllaDB do not allow INSERT against counter tables,
        // so INSERT + COUNTER batch type can never succeed and should fail validation up front.
        runner.setProperty(PutCQLRecord.BATCH_STATEMENT_TYPE, "COUNTER");

        runner.assertNotValid();
    }

    @Test
    @DisplayName("An INSERT with a COUNTER batch statement type resolved from a FlowFile attribute routes to failure at runtime")
    void testInsertWithCounterBatchTypeAttributeRoutesToFailure() {
        // When Batch Statement Type is resolved from a FlowFile attribute, the invalid INSERT + COUNTER
        // combination can't be caught at validation time, so it must be rejected at runtime instead.
        runner.setProperty(PutCQLRecord.BATCH_STATEMENT_TYPE, "USE_ATTR");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("", Map.of(PutCQLRecord.BATCH_STATEMENT_TYPE_ATTRIBUTE, "COUNTER"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_FAILURE, 1);
    }

    @Test
    @DisplayName("A record that fails to insert routes the FlowFile to the failure relationship")
    void testErrorHandler() {
        final int recordCount = 1;

        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        for (int i = 0; i < recordCount; i++) {
            mockReader.addRecord("Hello, world", "test_user");
        }

        doThrow(new ProcessException("Test"))
                .when(service)
                .insert(any(QualifiedTableName.class), anyList(), anyMap(), any(CqlBatchType.class), any(WriteOverrides.class));
        runner.enqueue("");
        runner.run();
        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_FAILURE, 1);
    }

    @Test
    @DisplayName("A Cassandra query failure while inserting routes the FlowFile to retry rather than failure")
    void testInsertQueryFailureRoutesToRetry() {
        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        mockReader.addRecord("Hello, world", "test_user");

        doThrow(new QueryFailureException())
                .when(service)
                .insert(any(QualifiedTableName.class), anyList(), anyMap(), any(CqlBatchType.class), any(WriteOverrides.class));
        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_RETRY, 1);
    }

    @Test
    @DisplayName("A Cassandra query failure while updating routes the FlowFile to retry rather than failure")
    void testUpdateQueryFailureRoutesToRetry() {
        runner.setProperty(PutCQLRecord.UPDATE_KEYS, "sender");
        runner.setProperty(PutCQLRecord.STATEMENT_TYPE, UPDATE_TYPE.getValue());
        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        mockReader.addRecord("Hello, world", "test_user");

        doThrow(new QueryFailureException())
                .when(service)
                .update(any(QualifiedTableName.class), anyList(), anyMap(), eq(List.of("sender")), eq(UpdateMethod.SET), any(CqlBatchType.class), any(WriteOverrides.class));
        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_RETRY, 1);
    }

    @Test
    @DisplayName("Each batch emits its own provenance event, and the final one reports the true total records written")
    void testProvenanceReportsRecordsWrittenPerBatchAndTotal() {
        final int recordCount = 250;

        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        for (int i = 0; i < recordCount; i++) {
            mockReader.addRecord("Hello, world", "test_user");
        }

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertEquals(3, events.size());
        assertTrue(events.get(0).getDetails().contains("100 total"));
        assertTrue(events.get(1).getDetails().contains("200 total"));
        assertTrue(events.get(2).getDetails().contains("250 total"));
    }

    @Test
    @DisplayName("A failure partway through writing batches leaves a provenance trail and an attribute for what was already written")
    void testPartialBatchFailureRecordsProgress() {
        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        for (int i = 0; i < 250; i++) {
            mockReader.addRecord("Hello, world", "test_user");
        }

        // First batch succeeds, second batch fails; the third batch is never reached.
        doNothing().doThrow(new ProcessException("Test"))
                .when(service)
                .insert(any(QualifiedTableName.class), anyList(), anyMap(), any(CqlBatchType.class), any(WriteOverrides.class));

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_FAILURE, 1);

        MockFlowFile failedFlowFile = runner.getFlowFilesForRelationship(PutCQLRecord.REL_FAILURE).get(0);
        assertEquals("100", failedFlowFile.getAttribute(PutCQLRecord.RECORDS_WRITTEN_ATTRIBUTE));

        List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertEquals(1, events.size());
        assertTrue(events.get(0).getDetails().contains("100 total"));
    }

    @Test
    @DisplayName("A dynamic property named keyspace.table.field is accepted and described in terms of that field/table/keyspace")
    void testDynamicPropertyDescriptorAcceptsQualifiedFieldName() {
        PutCQLRecord processor = new PutCQLRecord();

        PropertyDescriptor descriptor = processor.getSupportedDynamicPropertyDescriptor("my_keyspace.my_table.id_field");

        assertTrue(descriptor.isDynamic());
        assertEquals("my_keyspace.my_table.id_field", descriptor.getName());
        assertEquals("This property sets a record path to be evaluated for field id_field on table my_keyspace.my_table",
                descriptor.getDescription());
    }

    @ParameterizedTest
    @DisplayName("A dynamic property name that isn't keyspace.table.field is rejected")
    @ValueSource(strings = {
            "id_field",                          // missing keyspace and table
            "my_table.id_field",                 // missing keyspace
            "my_keyspace.my_table.extra.id_field", // too many segments
            "",                                   // empty
            "my_keyspace.my_table.",              // trailing dot, empty field
            ".my_table.id_field",                 // leading dot, empty keyspace
            "1keyspace.my_table.id_field",        // segment starting with a digit
            "my-keyspace.my_table.id_field",      // hyphen is not a valid CQL identifier character
            "my_keyspace.my_table.id field"       // whitespace inside a segment
    })
    void testDynamicPropertyDescriptorRejectsNonQualifiedName(String propertyName) {
        PutCQLRecord processor = new PutCQLRecord();

        assertThrows(IllegalArgumentException.class, () -> processor.getSupportedDynamicPropertyDescriptor(propertyName));
    }

    @Test
    @DisplayName("Rejecting an invalid dynamic property name reports which name was invalid")
    void testDynamicPropertyDescriptorRejectionMessageNamesTheInvalidProperty() {
        PutCQLRecord processor = new PutCQLRecord();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> processor.getSupportedDynamicPropertyDescriptor("not_qualified"));

        assertTrue(exception.getMessage().contains("not_qualified"),
                "Expected the exception message to name the invalid property, but was: " + exception.getMessage());
    }

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("A dynamic property RecordPath override for the target table is passed through as a primary key override on insert")
    void testDynamicPropertyOverrideAppliesToInsert() {
        runner.setProperty(PutCQLRecord.TABLE, "my_keyspace.message");
        runner.setProperty("my_keyspace.message.id", "/sender");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        final ArgumentCaptor<Map<PrimaryKeyIdentifier, RecordPath>> captor = ArgumentCaptor.forClass(Map.class);
        verify(service).insert(eq(new QualifiedTableName("my_keyspace", "message")), anyList(), captor.capture(),
                eq(CqlBatchType.LOGGED), eq(WriteOverrides.NONE));

        final Map<PrimaryKeyIdentifier, RecordPath> overrides = captor.getValue();
        final PrimaryKeyIdentifier identifier = new PrimaryKeyIdentifier("my_keyspace", "message", "id");
        assertEquals(1, overrides.size());
        assertTrue(overrides.containsKey(identifier));
        assertEquals("/sender", overrides.get(identifier).getPath());
    }

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("Multiple dynamic property RecordPath overrides for the same table are all passed through together")
    void testMultipleDynamicPropertyOverridesApplyToInsert() {
        runner.setProperty(PutCQLRecord.TABLE, "my_keyspace.message");
        runner.setProperty("my_keyspace.message.id", "/sender");
        runner.setProperty("my_keyspace.message.ts", "/message");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        final ArgumentCaptor<Map<PrimaryKeyIdentifier, RecordPath>> captor = ArgumentCaptor.forClass(Map.class);
        verify(service).insert(eq(new QualifiedTableName("my_keyspace", "message")), anyList(), captor.capture(),
                eq(CqlBatchType.LOGGED), eq(WriteOverrides.NONE));

        final Map<PrimaryKeyIdentifier, RecordPath> overrides = captor.getValue();
        assertEquals(2, overrides.size());
        assertEquals("/sender", overrides.get(new PrimaryKeyIdentifier("my_keyspace", "message", "id")).getPath());
        assertEquals("/message", overrides.get(new PrimaryKeyIdentifier("my_keyspace", "message", "ts")).getPath());
    }

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("A dynamic property RecordPath override for a different table is excluded from the overrides passed for the current write")
    void testDynamicPropertyOverrideForOtherTableIsExcluded() {
        runner.setProperty(PutCQLRecord.TABLE, "my_keyspace.message");
        runner.setProperty("my_keyspace.other_table.id", "/sender");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        final ArgumentCaptor<Map<PrimaryKeyIdentifier, RecordPath>> captor = ArgumentCaptor.forClass(Map.class);
        verify(service).insert(eq(new QualifiedTableName("my_keyspace", "message")), anyList(), captor.capture(),
                eq(CqlBatchType.LOGGED), eq(WriteOverrides.NONE));

        assertTrue(captor.getValue().isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("An unqualified Table name never matches any configured dynamic property override, since overrides always require a keyspace segment")
    void testDynamicPropertyOverrideIgnoredWhenTableIsUnqualified() {
        // TABLE is left at its unqualified default of "message" (set in setUp()), so the QualifiedTableName
        // built at runtime has a null keyspace - which can never equal a dynamic property's required,
        // non-null keyspace segment, so the override is silently never applied.
        runner.setProperty("my_keyspace.message.id", "/sender");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        final ArgumentCaptor<Map<PrimaryKeyIdentifier, RecordPath>> captor = ArgumentCaptor.forClass(Map.class);
        verify(service).insert(eq(new QualifiedTableName(null, "message")), anyList(), captor.capture(),
                eq(CqlBatchType.LOGGED), eq(WriteOverrides.NONE));

        assertTrue(captor.getValue().isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("A dynamic property RecordPath override for the target table is passed through as a primary key override on update")
    void testDynamicPropertyOverrideAppliesToUpdate() {
        runner.setProperty(PutCQLRecord.TABLE, "my_keyspace.message");
        runner.setProperty(PutCQLRecord.UPDATE_KEYS, "sender");
        runner.setProperty(PutCQLRecord.STATEMENT_TYPE, UPDATE_TYPE.getValue());
        runner.setProperty("my_keyspace.message.id", "/message");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        final ArgumentCaptor<Map<PrimaryKeyIdentifier, RecordPath>> captor = ArgumentCaptor.forClass(Map.class);
        verify(service).update(eq(new QualifiedTableName("my_keyspace", "message")), anyList(), captor.capture(),
                eq(List.of("sender")), eq(UpdateMethod.SET), eq(CqlBatchType.LOGGED), eq(WriteOverrides.NONE));

        final Map<PrimaryKeyIdentifier, RecordPath> overrides = captor.getValue();
        final PrimaryKeyIdentifier identifier = new PrimaryKeyIdentifier("my_keyspace", "message", "id");
        assertEquals(1, overrides.size());
        assertEquals("/message", overrides.get(identifier).getPath());
    }
}
