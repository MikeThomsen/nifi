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

import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.service.cql.it.AbstractCqlConnectionVerificationIT;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.BeforeParameterizedClassInvocation;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Connection/{@code verify()} coverage from {@link AbstractCqlConnectionVerificationIT} run against a real
 * Cassandra container. Real-credential authentication (against a container with {@code
 * PasswordAuthenticator} enabled) is covered separately in {@link CassandraAuthenticationIT},
 * since it requires a dedicated, differently-configured container per test. Runs against every supported
 * Cassandra major version when {@code -DTEST_CASSANDRA_OLDER_VERSIONS=true} is set; otherwise only the
 * current major version runs (see {@link CassandraTestVersions}).
 *
 * <p>The container itself belongs to {@link SharedCassandraCluster}, not to this class. Nothing here writes
 * to a table - every test verifies a configuration - so sharing costs this suite nothing, and it reads the
 * {@code testspace} keyspace {@code init.cql} creates at container start.
 */
@ParameterizedClass
@MethodSource("org.apache.nifi.service.cassandra.CassandraTestVersions#allVersions")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CassandraConnectionVerificationIT extends AbstractCqlConnectionVerificationIT {

    private static final String KEYSPACE = "testspace";

    // A @ParameterizedClass combined with @TestInstance(PER_CLASS) requires field injection rather than
    // constructor injection, so this field is what JUnit populates per invocation (per version value).
    @Parameter
    private String version;

    // Runs once per invocation of this parameterized class (once per version value), before any @Test
    // methods. Also responsible for calling initializeConnectionInfo() directly (rather than relying on an
    // inherited @BeforeAll) since @BeforeAll - even inherited from AbstractCqlConnectionVerificationIT -
    // runs BEFORE this method on a @ParameterizedClass leaf, not after.
    @BeforeParameterizedClassInvocation
    void attachToCluster(final String version) {
        initializeConnectionInfo(SharedCassandraCluster.forVersion(version).connectionInfo(KEYSPACE));
    }

    @Override
    protected CQLExecutionService newSessionProvider() {
        return new CassandraCQLExecutionService();
    }
}
