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

import org.apache.nifi.service.cql.api.CQLExecutionService;
import org.apache.nifi.service.cql.it.AbstractCqlSslIT;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.X509Certificate;

/**
 * {@code SSL Context Service} coverage from {@link AbstractCqlSslIT} run against a real Cassandra
 * container. Cassandra's {@code client_encryption_options} TLS config is Java-keystore-based (unlike
 * ScyllaDB's PEM-based config - see {@link org.apache.nifi.service.scylladb.ScyllaSslIT}) and has an
 * {@code optional: true} mode, so the {@code init.cql} script can still run over plaintext during container
 * startup even with encryption enabled - no TLS-only bootstrap session is needed the way ScyllaDB's
 * equivalent requires. Runs against a single Cassandra version rather than every supported major version,
 * since the property being exercised here is the Java Driver's SSL wiring rather than anything
 * version-specific (matching {@link CassandraRecordFieldTypeIT}'s rationale).
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CassandraSslIT extends AbstractCqlSslIT {

    private static final String CASSANDRA_VERSION = "5.0";

    private static final String CASSANDRA_KEYSTORE_CONTAINER_PATH = "/etc/cassandra/.keystore";

    private static final String CASSANDRA_TRUSTSTORE_CONTAINER_PATH = "/etc/cassandra/.truststore";

    @Override
    protected CQLExecutionService newSessionProvider() {
        return new CassandraCQLExecutionService();
    }

    @Override
    protected RunningSslContainer startOneWayServer(final KeyPair serverKeyPair, final X509Certificate serverCertificate) throws Exception {
        return startServer(serverKeyPair, serverCertificate, false, null);
    }

    @Override
    protected RunningSslContainer startTwoWayServer(final KeyPair serverKeyPair, final X509Certificate serverCertificate,
                                                      final KeyPair trustedClientKeyPair, final X509Certificate trustedClientCertificate) throws Exception {
        // Cassandra's init.cql runs over plaintext (client_encryption_options is "optional"), so unlike
        // ScyllaDB, no TLS bootstrap identity is needed here - trustedClientKeyPair is unused.
        return startServer(serverKeyPair, serverCertificate, true, trustedClientCertificate);
    }

    private RunningSslContainer startServer(final KeyPair serverKeyPair, final X509Certificate serverCertificate,
                                             final boolean requireClientAuth, final X509Certificate trustedClientCertificate) throws Exception {
        final SslKeyStoreCredentials serverKeyStore = writeKeyStore(serverKeyPair.getPrivate(), serverCertificate, "server");
        final SslKeyStoreCredentials serverTrustStore = requireClientAuth
                ? writeTrustStore(trustedClientCertificate, "client")
                : null;

        final CassandraContainer container = new CassandraContainer("cassandra:" + CASSANDRA_VERSION)
                .withCopyFileToContainer(MountableFile.forHostPath(writeSslPatchedConfig(requireClientAuth, serverKeyStore, serverTrustStore)),
                        CassandraConfigOverrides.CONTAINER_CASSANDRA_YAML_PATH)
                .withCopyFileToContainer(MountableFile.forHostPath(serverKeyStore.path()), CASSANDRA_KEYSTORE_CONTAINER_PATH)
                .withInitScript("init.cql");

        if (serverTrustStore != null) {
            container.withCopyFileToContainer(MountableFile.forHostPath(serverTrustStore.path()), CASSANDRA_TRUSTSTORE_CONTAINER_PATH);
        }
        container.withExposedPorts(9042);
        container.start();

        final String contactPoint = container.getContainerIpAddress() + ":" + container.getMappedPort(9042);
        return new RunningSslContainer(contactPoint, container::stop);
    }

    // Patches the client_encryption_options block of the shared cassandra-base-config/cassandra.yaml (which
    // ships with it disabled) rather than checking in a near-duplicate copy of that ~250-line file for each
    // of one-way and two-way TLS.
    private static Path writeSslPatchedConfig(final boolean requireClientAuth, final SslKeyStoreCredentials serverKeyStore,
                                               final SslKeyStoreCredentials serverTrustStore) {
        return CassandraConfigOverrides.writePatchedConfig(config -> {
            final StringBuilder block = new StringBuilder("client_encryption_options:\n")
                    .append("  enabled: true\n")
                    .append("  optional: true\n")
                    .append("  keystore: ").append(CASSANDRA_KEYSTORE_CONTAINER_PATH).append('\n')
                    .append("  keystore_password: ").append(serverKeyStore.password()).append('\n')
                    .append("  require_client_auth: ").append(requireClientAuth).append('\n');
            if (requireClientAuth) {
                block.append("  truststore: ").append(CASSANDRA_TRUSTSTORE_CONTAINER_PATH).append('\n')
                        .append("  truststore_password: ").append(serverTrustStore.password()).append('\n');
            }
            block.append("  store_type: ").append(STORE_TYPE).append('\n');

            return config.replace(
                    "client_encryption_options:\n\n  enabled: false\n\n  keystore: conf/.keystore\n\n  require_client_auth: false\n",
                    block.toString());
        });
    }
}
