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

package org.apache.nifi.service.scylladb;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.service.cql.it.AbstractCqlCrudIT;
import org.apache.nifi.service.cql.it.CqlConnectionInfo;
import org.apache.nifi.service.cql.it.CqlDdl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.scylladb.ScyllaDBContainer;

/**
 * CRUD coverage from {@link AbstractCqlCrudIT} run against a real ScyllaDB container. Unlike the Cassandra
 * side, this runs against a single, fixed image version rather than a parameterized matrix - see the
 * refactoring plan for why. Schema bootstrap uses inline {@code session.execute()} calls rather than
 * {@code withInitScript}, since {@link ScyllaDBContainer} (a plain {@code GenericContainer} subclass) has no
 * equivalent hook. Uses {@link ScyllaDdlTimeouts} for the same reason as {@link ScyllaRecordFieldTypeIT}.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScyllaCrudIT extends AbstractCqlCrudIT {

    private static final String IMAGE = "scylladb/scylla:2026.2";

    private static final String KEYSPACE = "testspace";

    private static final String LOCAL_DATACENTER = "datacenter1";

    private ScyllaDBContainer container;

    private CqlSession session;

    @BeforeAll
    void startContainer() throws Exception {
        container = new ScyllaDBContainer(IMAGE);
        container.withExposedPorts(9042);
        container.start();

        final String contactPoint = container.getContainerIpAddress() + ":" + container.getMappedPort(9042);
        session = CqlSession.builder()
                .addContactPoint(container.getContactPoint())
                .withLocalDatacenter(LOCAL_DATACENTER)
                .withConfigLoader(ScyllaDdlTimeouts.longSchemaTimeoutConfigLoader())
                .build();

        CqlDdl.executeWithRetry(session,
                "create keyspace if not exists " + KEYSPACE + " with replication = { 'class': 'NetworkTopologyStrategy', '" + LOCAL_DATACENTER + "': 1};");
        CqlDdl.executeWithRetry(session, """
                create table if not exists testspace.message
                (
                    sender    text,
                    receiver  text,
                    message   text,
                    when_sent timestamp,
                    primary key ( sender, receiver, when_sent )
                );
                """);
        CqlDdl.executeWithRetry(session, """
                create table if not exists testspace.query_test
                (
                    column_a text,
                    column_b text,
                    when     timestamp,
                    primary key ( (column_a), column_b)
                );
                """);
        CqlDdl.executeWithRetry(session, """
                create table if not exists testspace.counter_test
                (
                    column_a        text,
                    increment_field counter,
                    primary key ( column_a )
                );
                """);
        CqlDdl.executeWithRetry(session, """
                create table if not exists testspace.simple_set_test
                (
                    username text,
                    is_active boolean,
                    primary key ( username )
                );""");

        initializeSessionProvider(new CqlConnectionInfo(contactPoint, LOCAL_DATACENTER, KEYSPACE, session));
    }

    @AfterAll
    void tearDown() {
        session.close();
        container.stop();
    }

    @Override
    protected CQLExecutionService newSessionProvider() {
        return new ScyllaDBCQLExecutionService();
    }
}
