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
import org.apache.nifi.service.cql.it.AbstractCqlRecordFieldTypeIT;
import org.apache.nifi.service.cql.it.CqlConnectionInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Type-coverage tests from {@link AbstractCqlRecordFieldTypeIT} run against a real Cassandra container.
 * Single fixed image version rather than a parameterized matrix, since the behavior under test (record
 * field type <-> CQL type conversion) doesn't vary across Cassandra major versions the way connection/auth
 * handling can.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CassandraRecordFieldTypeIT extends AbstractCqlRecordFieldTypeIT {

    private static final String CASSANDRA_IMAGE = "cassandra:5.0";

    private static final String KEYSPACE = "type_coverage";

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
