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
import org.apache.nifi.service.cql.it.AbstractCqlAuthenticationIT;
import org.apache.nifi.service.cql.it.CqlDdl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.scylladb.ScyllaDBContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Real-credential authentication coverage from {@link AbstractCqlAuthenticationIT} run against a real
 * ScyllaDB container with {@code PasswordAuthenticator} enabled.
 * <p>
 * Starting with ScyllaDB 2026.2, the image no longer provisions a default superuser automatically (confirmed
 * empirically in earlier investigation: no role-related activity appears anywhere in the container logs, and
 * no combination of default credentials authenticates, even after retrying for minutes). Instead, a custom
 * bootstrap superuser is provisioned directly via {@code scylla.yaml}'s {@code auth_superuser_name} /
 * {@code auth_superuser_salted_password} settings, appended at runtime onto the same checked-in
 * {@code scylla-ssl-base-config/scylla.yaml} template {@link ScyllaSslIT} builds from (see {@link
 * #buildAuthYaml()}), rather than a separate near-duplicate config file. The salted password is a
 * pre-computed SHA-512-crypt ({@code $6$}) hash - the same scheme Scylla's own
 * {@code password_authenticator} logs using at startup. That bootstrap identity's password is a fixed,
 * non-secret placeholder (this container is never reachable outside the test), used only once to create the
 * real, randomly-generated-password {@code admin} role {@link AbstractCqlAuthenticationIT}'s tests verify -
 * the same two-step shape as Cassandra's {@link org.apache.nifi.service.cassandra.CassandraAuthenticationIT},
 * just with a self-provisioned bootstrap identity standing in for Cassandra's built-in default superuser.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScyllaAuthenticationIT extends AbstractCqlAuthenticationIT {

    private static final String IMAGE = "scylladb/scylla:2026.2";

    private static final String KEYSPACE = "testspace";

    private static final String LOCAL_DATACENTER = "datacenter1";

    private static final String BASE_CONFIG_RESOURCE = "scylla-ssl-base-config/scylla.yaml";

    private static final String SCYLLA_YAML_CONTAINER_PATH = "/etc/scylla/scylla.yaml";

    private static final String BOOTSTRAP_USERNAME = "bootstrap_admin";

    private static final String BOOTSTRAP_PASSWORD = "cassandra-bootstrap-only";

    // The SHA-512-crypt hash of BOOTSTRAP_PASSWORD above, in the form auth_superuser_salted_password expects.
    private static final String BOOTSTRAP_SALTED_PASSWORD =
            "$6$Yk/rTi56stmYje0c$JfFJrNQa5wUQAV/ZyyHqaAtGs.4CLcHbLh6RANryG6/pxeHvmPO0/eA8mSUY5HHs5w9sNFtwp.008MCHKo5ty.";

    private ScyllaDBContainer container;

    @BeforeAll
    void startContainerAndCreateRole() throws Exception {
        container = new ScyllaDBContainer(IMAGE)
                .withCopyFileToContainer(MountableFile.forHostPath(buildAuthYaml()), SCYLLA_YAML_CONTAINER_PATH);
        container.withExposedPorts(9042);
        container.start();

        final String contactPoint = container.getContainerIpAddress() + ":" + container.getMappedPort(9042);
        final String realPassword = UUID.randomUUID().toString();

        // CREATE ROLE and create keyspace are schema-modifying DDL, which is slower to settle than plain
        // reads/writes - the driver's built-in 2 second defaults for both request timeouts and internal
        // schema-refresh/agreement queries (issued automatically after every DDL statement) aren't always
        // enough, so this bootstrap session needs longer timeouts across the board (see ScyllaDdlTimeouts,
        // shared with ScyllaRecordFieldTypeIT, which hits the exact same issue).
        final DriverConfigLoader longTimeoutConfig = ScyllaDdlTimeouts.longSchemaTimeoutConfigLoader();

        try (CqlSession bootstrapSession = CqlSession.builder()
                .addContactPoint(container.getContactPoint())
                .withLocalDatacenter(LOCAL_DATACENTER)
                .withAuthCredentials(BOOTSTRAP_USERNAME, BOOTSTRAP_PASSWORD)
                .withConfigLoader(longTimeoutConfig)
                .build()) {
            CqlDdl.executeWithRetry(bootstrapSession, String.format(
                    "CREATE ROLE IF NOT EXISTS admin WITH PASSWORD = '%s' AND LOGIN = true AND SUPERUSER = true", realPassword));
            CqlDdl.executeWithRetry(bootstrapSession, "create keyspace if not exists " + KEYSPACE
                    + " with replication = { 'class': 'NetworkTopologyStrategy', '" + LOCAL_DATACENTER + "': 1};");
        }

        initializeAuthConnectionInfo(contactPoint, LOCAL_DATACENTER, KEYSPACE, realPassword);
    }

    @AfterAll
    void tearDown() {
        container.stop();
    }

    /**
     * Builds the server's {@code scylla.yaml} by appending {@code authenticator}/{@code auth_superuser_*}
     * settings onto the checked-in {@code scylla-ssl-base-config/scylla.yaml} template - the same base
     * {@link ScyllaSslIT} builds its {@code client_encryption_options} variants from - rather than checking in
     * a separate, near-duplicate config file for this one setting.
     */
    private Path buildAuthYaml() throws IOException {
        final String baseYaml;
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(BASE_CONFIG_RESOURCE)) {
            if (inputStream == null) {
                throw new IOException("Classpath resource not found: " + BASE_CONFIG_RESOURCE);
            }
            baseYaml = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }

        final String authOptions = "authenticator: PasswordAuthenticator\n"
                + "auth_superuser_name: " + BOOTSTRAP_USERNAME + "\n"
                + "auth_superuser_salted_password: \"" + BOOTSTRAP_SALTED_PASSWORD + "\"\n";

        final Path directory = Files.createDirectories(Paths.get("target", "scylla-auth-it", UUID.randomUUID().toString()));
        final Path yamlFile = directory.resolve("scylla.yaml");
        Files.writeString(yamlFile, baseYaml + "\n" + authOptions);
        return yamlFile;
    }

    @Override
    protected CQLExecutionService newSessionProvider() {
        return new ScyllaDBCQLExecutionService();
    }
}
