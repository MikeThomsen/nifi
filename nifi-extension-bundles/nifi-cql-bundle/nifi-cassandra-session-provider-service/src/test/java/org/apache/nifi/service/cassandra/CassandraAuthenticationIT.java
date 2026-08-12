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
import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.service.cql.it.AbstractCqlAuthenticationIT;
import org.apache.nifi.service.cql.it.DockerUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.BeforeParameterizedClassInvocation;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.util.UUID;

/**
 * Real-credential authentication coverage from {@link AbstractCqlAuthenticationIT} run against a real
 * Cassandra container with {@code PasswordAuthenticator} enabled. This needs a dedicated,
 * differently-configured container per class (server-side auth config can't be toggled on a running
 * container), so it isn't a good fit for the shared container used by {@link CassandraConnectionVerificationIT},
 * which covers the rest of {@code verify()} and driver-config-file handling. Runs against every supported
 * Cassandra major version when {@code -DTEST_CASSANDRA_OLDER_VERSIONS=true} is set; otherwise only the
 * current major version runs (see {@link CassandraTestVersions}).
 */
@Testcontainers
@ParameterizedClass
@MethodSource("org.apache.nifi.service.cassandra.CassandraTestVersions#allVersions")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CassandraAuthenticationIT extends AbstractCqlAuthenticationIT {

    private static final String KEYSPACE = "testspace";

    private static final String LOCAL_DATACENTER = "datacenter1";

    // resources/cassandra-auth-config-<version>/cassandra.yaml overrides for 3.11 and 4.1 - each is a full
    // copy of that version's own stock cassandra.yaml with only authenticator: PasswordAuthenticator
    // changed. CassandraContainer#withConfigurationOverride() replaces the entire /etc/cassandra directory
    // rather than merging into it, and the stock yaml's schema differs across major versions, so a single
    // shared/minimal override file can't be used across versions. 5.0 instead patches
    // cassandra-base-config/cassandra.yaml at runtime via CassandraConfigOverrides, since that base is
    // already shared with the other 5.0-only IT classes in this package.
    private static final String AUTH_CONFIG_OVERRIDE_PREFIX = "cassandra-auth-config-";

    private static final String CASSANDRA_5_VERSION = "5.0";

    // A @ParameterizedClass combined with @TestInstance(PER_CLASS) requires field injection rather than
    // constructor injection, so this field is what JUnit populates per invocation (per version value).
    @Parameter
    private String version;

    private CassandraContainer container;

    // Runs once per invocation of this parameterized class (once per version value), before any @Test
    // methods - including the ones inherited from AbstractCqlAuthenticationIT. Also responsible for calling
    // initializeAuthConnectionInfo() directly (rather than relying on an inherited @BeforeAll) since
    // @BeforeAll - even inherited from AbstractCqlAuthenticationIT - runs BEFORE this method on a
    // @ParameterizedClass leaf, not after; see AbstractCqlCrudIT's class javadoc for the same trap.
    @BeforeParameterizedClassInvocation
    void startContainerAndCreateRole(final String version) throws Exception {
        // A dedicated container per invocation: enabling PasswordAuthenticator is a server-side config
        // change (cassandra.yaml authenticator), so it can't be toggled on an already-running container.
        container = new CassandraContainer("cassandra:" + version)
                .withInitScript("init.cql")
                .withCreateContainerCmdModifier(DockerUtils.createMemoryLimits(2L, 2));
        if (CASSANDRA_5_VERSION.equals(version)) {
            container.withCopyFileToContainer(
                    MountableFile.forHostPath(CassandraConfigOverrides.writePatchedConfig(config ->
                            config.replace("authenticator: AllowAllAuthenticator", "authenticator: PasswordAuthenticator"))),
                    CassandraConfigOverrides.CONTAINER_CASSANDRA_YAML_PATH);
        } else {
            container.withConfigurationOverride(AUTH_CONFIG_OVERRIDE_PREFIX + version);
        }
        container.withExposedPorts(9042);
        container.start();

        final String realPassword = UUID.randomUUID().toString();

        // Cassandra ships a default superuser ("cassandra"/"cassandra") that testcontainers itself already
        // uses (CassandraContainer#getUsername/getPassword) to run the init script, so it's usable the
        // moment the container reports ready. Use it once, and only once, to create a dedicated "admin"
        // role with a real, generated password.
        try (CqlSession bootstrapSession = CqlSession.builder()
                .addContactPoint(container.getContactPoint())
                .withLocalDatacenter(LOCAL_DATACENTER)
                .withAuthCredentials(container.getUsername(), container.getPassword())
                .build()) {
            bootstrapSession.execute(String.format(
                    "CREATE ROLE admin WITH PASSWORD = '%s' AND LOGIN = true", realPassword));
        }

        final String contactPoint = container.getContainerIpAddress() + ":" + container.getMappedPort(9042);
        initializeAuthConnectionInfo(contactPoint, LOCAL_DATACENTER, KEYSPACE, realPassword);
    }

    @AfterAll
    void tearDown() {
        container.stop();
    }

    @Override
    protected CQLExecutionService newSessionProvider() {
        return new CassandraCQLExecutionService();
    }
}
