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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.google.common.collect.Sets;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.service.CassandraSessionProvider;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the AbstractCassandraProcessor class
 */
public class AbstractCassandraProcessorTest {

    MockAbstractCassandraProcessor processor;
    private TestRunner testRunner;

    @BeforeEach
    public void setUp() {
        processor = new MockAbstractCassandraProcessor();
        testRunner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testCustomValidateEL() {
        testRunner.setProperty(AbstractCassandraProcessor.CHARSET, "${charset}");
        testRunner.assertValid();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetCassandraObject() {
        Row row = CassandraQueryTestUtil.createRow("user1", "Joe", "Smith",
                Sets.newHashSet("jsmith@notareal.com", "joes@fakedomain.com"), Arrays.asList("New York, NY", "Santa Clara, CA"),
                new HashMap<Date, String>() {{
                    put(Calendar.getInstance().getTime(), "Set my alarm for a month from now");
                }}, true, 1.0f, 2.0);

        assertEquals("user1", AbstractCassandraProcessor.getCassandraObject(row, 0, DataType.text()));
        assertEquals("Joe", AbstractCassandraProcessor.getCassandraObject(row, 1, DataType.text()));
        assertEquals("Smith", AbstractCassandraProcessor.getCassandraObject(row, 2, DataType.text()));
        Set<String> emails = (Set<String>) AbstractCassandraProcessor.getCassandraObject(row, 3, DataType.set(DataType.text()));
        assertNotNull(emails);
        assertEquals(2, emails.size());
        List<String> topPlaces = (List<String>) AbstractCassandraProcessor.getCassandraObject(row, 4, DataType.list(DataType.text()));
        assertNotNull(topPlaces);
        Map<Date, String> todoMap = (Map<Date, String>) AbstractCassandraProcessor.getCassandraObject(
                row, 5, DataType.map(DataType.timestamp(), DataType.text()));
        assertNotNull(todoMap);
        assertEquals(1, todoMap.values().size());
        Boolean registered = (Boolean) AbstractCassandraProcessor.getCassandraObject(row, 6, DataType.cboolean());
        assertNotNull(registered);
        assertTrue(registered);
    }

    @Test
    public void testGetSchemaForType() {
        assertEquals(AbstractCassandraProcessor.getSchemaForType("string").getType().getName(), "string");
        assertEquals(AbstractCassandraProcessor.getSchemaForType("boolean").getType().getName(), "boolean");
        assertEquals(AbstractCassandraProcessor.getSchemaForType("int").getType().getName(), "int");
        assertEquals(AbstractCassandraProcessor.getSchemaForType("long").getType().getName(), "long");
        assertEquals(AbstractCassandraProcessor.getSchemaForType("float").getType().getName(), "float");
        assertEquals(AbstractCassandraProcessor.getSchemaForType("double").getType().getName(), "double");
        assertEquals(AbstractCassandraProcessor.getSchemaForType("bytes").getType().getName(), "bytes");
    }

    @Test
    public void testGetSchemaForTypeBadType() {
        assertThrows(IllegalArgumentException.class, () -> AbstractCassandraProcessor.getSchemaForType("nothing"));
    }

    @Test
    public void testGetPrimitiveAvroTypeFromCassandraType() {
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.ascii()));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.text()));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.varchar()));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.timestamp()));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.timeuuid()));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.uuid()));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.inet()));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.varint()));

        assertEquals("boolean", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.cboolean()));
        assertEquals("int", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.cint()));

        assertEquals("long", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.bigint()));
        assertEquals("long", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.counter()));

        assertEquals("float", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.cfloat()));
        assertEquals("double", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.cdouble()));

        assertEquals("bytes", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.blob()));
    }

    @Test
    public void testGetPrimitiveAvroTypeFromCassandraTypeBadType() {
        DataType mockDataType = mock(DataType.class);
        assertThrows(IllegalArgumentException.class, () -> AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(mockDataType));
    }

    @Test
    public void testGetPrimitiveDataTypeFromString() {
        assertEquals(DataType.ascii(), AbstractCassandraProcessor.getPrimitiveDataTypeFromString("ascii"));
    }

    @Test
    public void testGetContactPoints() {
        List<InetSocketAddress> contactPoints = processor.getContactPoints("");
        assertNotNull(contactPoints);
        assertEquals(1, contactPoints.size());
        assertEquals("localhost", contactPoints.get(0).getHostName());
        assertEquals(AbstractCassandraProcessor.DEFAULT_CASSANDRA_PORT, contactPoints.get(0).getPort());

        contactPoints = processor.getContactPoints("192.168.99.100:9042");
        assertNotNull(contactPoints);
        assertEquals(1, contactPoints.size());
        assertEquals("192.168.99.100", contactPoints.get(0).getAddress().getHostAddress());
        assertEquals(9042, contactPoints.get(0).getPort());

        contactPoints = processor.getContactPoints("192.168.99.100:9042, mydomain.com : 4000");
        assertNotNull(contactPoints);
        assertEquals(2, contactPoints.size());
        assertEquals("192.168.99.100", contactPoints.get(0).getAddress().getHostAddress());
        assertEquals(9042, contactPoints.get(0).getPort());
        assertEquals("mydomain.com", contactPoints.get(1).getHostName());
        assertEquals(4000, contactPoints.get(1).getPort());
    }

    @Test
    public void testCustomValidateCassandraConnectionConfiguration() throws InitializationException {
        MockCassandraSessionProvider sessionProviderService = new MockCassandraSessionProvider();

        testRunner.addControllerService("cassandra-connection-provider", sessionProviderService);
        testRunner.setProperty(sessionProviderService, CassandraSessionProvider.CONTACT_POINTS, "localhost:9042");
        testRunner.setProperty(sessionProviderService, CassandraSessionProvider.KEYSPACE, "somekyespace");

        testRunner.setProperty(AbstractCassandraProcessor.CONNECTION_PROVIDER_SERVICE, "cassandra-connection-provider");
        testRunner.enableControllerService(sessionProviderService);

        testRunner.assertNotValid();
        testRunner.assertValid();
    }

    /**
     * Provides a stubbed processor instance for testing
     */
    public static class MockAbstractCassandraProcessor extends AbstractCassandraProcessor {

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return Arrays.asList(CONNECTION_PROVIDER_SERVICE, CHARSET);
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        }

        @Override
        protected Cluster createCluster(List<InetSocketAddress> contactPoints, SSLContext sslContext,
                                        String username, String password, String compressionType) {
            Cluster mockCluster = mock(Cluster.class);
            Metadata mockMetadata = mock(Metadata.class);
            when(mockMetadata.getClusterName()).thenReturn("cluster1");
            when(mockCluster.getMetadata()).thenReturn(mockMetadata);
            Configuration config = Configuration.builder().build();
            when(mockCluster.getConfiguration()).thenReturn(config);
            return mockCluster;
        }

        public Cluster getCluster() {
            return cluster.get();
        }

        public void setCluster(Cluster newCluster) {
            this.cluster.set(newCluster);
        }
    }

    /**
     * Mock CassandraSessionProvider implementation for testing purpose
     */
    private class MockCassandraSessionProvider extends CassandraSessionProvider {

        @OnEnabled
        public void onEnabled(final ConfigurationContext context) {

        }

    }
}