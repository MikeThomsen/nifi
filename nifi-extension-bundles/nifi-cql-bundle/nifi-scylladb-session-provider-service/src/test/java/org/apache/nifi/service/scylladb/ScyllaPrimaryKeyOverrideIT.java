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
import org.apache.nifi.service.cql.api.CQLExecutionService;
import org.apache.nifi.service.cql.it.AbstractCqlPrimaryKeyOverrideIT;
import org.apache.nifi.service.cql.it.CqlConnectionInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.scylladb.ScyllaDBContainer;

/**
 * Primary key RecordPath override coverage from {@link AbstractCqlPrimaryKeyOverrideIT} run against a real
 * ScyllaDB container. Single fixed image version, not parameterized - see {@link ScyllaCrudIT}'s javadoc for
 * why.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScyllaPrimaryKeyOverrideIT extends AbstractCqlPrimaryKeyOverrideIT {

    private static final String IMAGE = "scylladb/scylla:2026.2";

    private static final String KEYSPACE = "pk_override";

    private static final String LOCAL_DATACENTER = "datacenter1";

    private ScyllaDBContainer container;

    private CqlSession session;

    @BeforeAll
    void startContainer() throws Exception {
        container = new ScyllaDBContainer(IMAGE);
        container.withExposedPorts(9042);
        container.start();

        // This class creates 3 tables up front and then issues 500+ individual inserts/point-lookups - the
        // table creation is schema-modifying DDL, which needs the same longer timeouts as every other
        // bootstrap session in this package (see ScyllaDdlTimeouts); the subsequent inserts/selects go
        // through sessionProvider's own separately-configured connection, same as every other IT here.
        final DriverConfigLoader longTimeoutConfig = ScyllaDdlTimeouts.longSchemaTimeoutConfigLoader();
        session = CqlSession.builder()
                .addContactPoint(container.getContactPoint())
                .withLocalDatacenter(LOCAL_DATACENTER)
                .withConfigLoader(longTimeoutConfig)
                .build();
        ScyllaDdlTimeouts.executeDdlWithRetry(session,
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
