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
import org.apache.nifi.service.cql.it.AbstractCqlCrudIT;
import org.apache.nifi.service.cql.it.CqlConnectionInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.BeforeParameterizedClassInvocation;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * CRUD coverage from {@link AbstractCqlCrudIT} run against a real Cassandra container. Runs against every
 * supported Cassandra major version when {@code -DTEST_CASSANDRA_OLDER_VERSIONS=true} is set; otherwise
 * only the current major version runs (see {@link CassandraTestVersions}).
 */
@Testcontainers
@ParameterizedClass
@MethodSource("org.apache.nifi.service.cassandra.CassandraTestVersions#allVersions")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CassandraCrudIT extends AbstractCqlCrudIT {

    private static final String KEYSPACE = "testspace";

    private static final String LOCAL_DATACENTER = "datacenter1";

    // A @ParameterizedClass combined with @TestInstance(PER_CLASS) requires field injection rather than
    // constructor injection, so this field is what JUnit populates per invocation (per version value).
    @Parameter
    private String version;

    private CassandraContainer container;

    private CqlSession session;

    // Runs once per invocation of this parameterized class (once per version value), before any @Test
    // methods. Also responsible for calling initializeSessionProvider() directly (rather than relying on
    // an inherited @BeforeAll) since @BeforeAll - even inherited from AbstractCqlCrudIT - runs BEFORE this
    // method on a @ParameterizedClass leaf, not after; see AbstractCqlCrudIT's class javadoc.
    @BeforeParameterizedClassInvocation
    void startContainer(final String version) throws Exception {
        container = new CassandraContainer("cassandra:" + version)
                .withInitScript("init.cql");
        container.withExposedPorts(9042);
        container.start();

        final String contactPoint = container.getContainerIpAddress() + ":" + container.getMappedPort(9042);
        session = CqlSession.builder()
                .addContactPoint(container.getContactPoint())
                .withLocalDatacenter(LOCAL_DATACENTER)
                .build();

        initializeSessionProvider(new CqlConnectionInfo(contactPoint, LOCAL_DATACENTER, KEYSPACE, session));
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
