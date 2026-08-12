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
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.service.cql.it.AbstractCqlRecordFieldTypeIT;
import org.apache.nifi.service.cql.it.CqlConnectionInfo;
import org.apache.nifi.service.cql.it.CqlDdl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.scylladb.ScyllaDBContainer;

/**
 * Type-coverage tests from {@link AbstractCqlRecordFieldTypeIT} run against a real ScyllaDB container.
 * Single fixed image version, not parameterized - see {@link ScyllaCrudIT}'s javadoc for why.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScyllaRecordFieldTypeIT extends AbstractCqlRecordFieldTypeIT {

    private static final String IMAGE = "scylladb/scylla:2026.2";

    private static final String KEYSPACE = "type_coverage";

    private static final String LOCAL_DATACENTER = "datacenter1";

    private ScyllaDBContainer container;

    private CqlSession session;

    @BeforeAll
    void startContainer() throws Exception {
        container = new ScyllaDBContainer(IMAGE);
        container.withExposedPorts(9042);
        container.start();

        // This class issues a rapid sequence of schema-modifying DDL (a "create table" or "create type" per
        // test, ~40 statements total), which is slower to settle than plain reads/writes - the driver's
        // built-in 2 second defaults for both request timeouts and internal schema-refresh/agreement queries
        // (issued automatically after every DDL statement) aren't enough here, unlike sessionProvider's own
        // 30 second default (CQLExecutionService.READ_TIMEOUT), so this bootstrap session needs its own
        // longer timeouts across the board (see ScyllaDdlTimeouts, shared with ScyllaAuthenticationIT, which
        // hits the exact same issue).
        final DriverConfigLoader longTimeoutConfig = ScyllaDdlTimeouts.longSchemaTimeoutConfigLoader();
        session = CqlSession.builder()
                .addContactPoint(container.getContactPoint())
                .withLocalDatacenter(LOCAL_DATACENTER)
                .withConfigLoader(longTimeoutConfig)
                .build();
        CqlDdl.executeWithRetry(session,
                "create keyspace if not exists " + KEYSPACE + " with replication = { 'class': 'NetworkTopologyStrategy', '" + LOCAL_DATACENTER + "': 1};");

        final String contactPoint = container.getContainerIpAddress() + ":" + container.getMappedPort(9042);

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
