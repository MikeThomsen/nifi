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

package org.apache.nifi.service.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.nifi.service.cql.api.CQLExecutionService;
import org.apache.nifi.service.cql.it.AbstractCqlPrimaryKeyOverrideIT;
import org.apache.nifi.service.cql.it.CqlConnectionInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Primary key RecordPath override coverage from {@link AbstractCqlPrimaryKeyOverrideIT} run against a real
 * Cassandra container. Single fixed image version, not parameterized across Cassandra major versions - see
 * {@link CassandraRecordFieldTypeIT}'s javadoc for why (this test exercises the same kind of
 * version-independent behavior: statement generation and codec-driven type coercion, not anything that
 * varies release to release).
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CassandraPrimaryKeyOverrideIT extends AbstractCqlPrimaryKeyOverrideIT {

    private static final String CASSANDRA_IMAGE = "cassandra:5.0";

    private static final String KEYSPACE = "pk_override";

    private static final String DATACENTER = "datacenter1";

    private CassandraContainer container;

    private CqlSession session;

    @BeforeAll
    void startContainer() throws Exception {
        container = new CassandraContainer(CASSANDRA_IMAGE);
        container.withExposedPorts(9042);
        container.start();

        session = CqlSession.builder()
                .addContactPoint(container.getContactPoint())
                .withLocalDatacenter(DATACENTER)
                .build();
        session.execute("create keyspace " + KEYSPACE + " with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

        final String contactPoint = container.getContainerIpAddress() + ":" + container.getMappedPort(9042);

        initializeSessionProvider(new CqlConnectionInfo(contactPoint, DATACENTER, KEYSPACE, session));
    }

    @AfterAll
    void tearDown() {
        session.close();
        container.stop();
    }

    @Override
    protected CQLExecutionService newSessionProvider() {
        return new CassandraCQLExecutionService();
    }
}
