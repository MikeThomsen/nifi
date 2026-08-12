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
import org.apache.nifi.service.cql.it.AbstractCqlSslIT;
import org.apache.nifi.service.cql.it.CqlDdl;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.scylladb.ScyllaDBContainer;
import org.testcontainers.utility.MountableFile;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.UUID;

/**
 * {@code SSL Context Service} coverage from {@link AbstractCqlSslIT} run against a real ScyllaDB container.
 * Unlike Cassandra's Java-keystore-based {@code client_encryption_options}
 * (see {@link org.apache.nifi.service.cassandra.CassandraSslIT}), ScyllaDB's server-side TLS config is
 * PEM-based ({@code certificate}/{@code keyfile}/{@code truststore}) and has no "optional" mode - a
 * plaintext connection is refused outright once {@code enabled: true} is set - so the test keyspace has to
 * be created over a TLS-only bootstrap session ({@link #createTestKeyspaceOverTls}) rather than via
 * {@code withInitScript}, using the same real server/client identities the container is actually configured
 * to trust regardless of which test scenario (success or failure) triggered the container startup.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScyllaSslIT extends AbstractCqlSslIT {

    private static final String IMAGE = "scylladb/scylla:2026.2";

    private static final String BASE_CONFIG_RESOURCE = "scylla-ssl-base-config/scylla.yaml";

    private static final String SERVER_CERT_CONTAINER_PATH = "/etc/scylla/ssl/server.crt";

    private static final String SERVER_KEY_CONTAINER_PATH = "/etc/scylla/ssl/server.key";

    private static final String SERVER_TRUSTSTORE_CONTAINER_PATH = "/etc/scylla/ssl/client-truststore.pem";

    private static final String SCYLLA_YAML_CONTAINER_PATH = "/etc/scylla/scylla.yaml";

    private static final String CERTIFICATE_PEM_FORMAT = "-----BEGIN CERTIFICATE-----%n%s%n-----END CERTIFICATE-----%n";

    private static final String PRIVATE_KEY_PEM_FORMAT = "-----BEGIN PRIVATE KEY-----%n%s%n-----END PRIVATE KEY-----%n";

    @Override
    protected CQLExecutionService newSessionProvider() {
        return new ScyllaDBCQLExecutionService();
    }

    @Override
    protected RunningSslContainer startOneWayServer(final KeyPair serverKeyPair, final X509Certificate serverCertificate) throws Exception {
        return startServer(serverKeyPair, serverCertificate, false, null, null);
    }

    @Override
    protected RunningSslContainer startTwoWayServer(final KeyPair serverKeyPair, final X509Certificate serverCertificate,
                                                      final KeyPair trustedClientKeyPair, final X509Certificate trustedClientCertificate) throws Exception {
        return startServer(serverKeyPair, serverCertificate, true, trustedClientKeyPair, trustedClientCertificate);
    }

    private RunningSslContainer startServer(final KeyPair serverKeyPair, final X509Certificate serverCertificate, final boolean requireClientAuth,
                                             final KeyPair trustedClientKeyPair, final X509Certificate trustedClientCertificate) throws Exception {
        final Path targetDirectory = createTargetDirectory(requireClientAuth ? "twoway" : "oneway");

        final Path serverCertPem = writeCertificatePem(serverCertificate, targetDirectory, "server-cert.pem");
        final Path serverKeyPem = writePrivateKeyPem(serverKeyPair.getPrivate(), targetDirectory, "server-key.pem");

        Path serverTrustStorePem = null;
        if (requireClientAuth) {
            serverTrustStorePem = writeCertificatePem(trustedClientCertificate, targetDirectory, "client-truststore.pem");
        }

        final Path serverYaml = buildServerYaml(targetDirectory, requireClientAuth);

        final ScyllaDBContainer container = new ScyllaDBContainer(IMAGE)
                .withCopyFileToContainer(MountableFile.forHostPath(serverYaml), SCYLLA_YAML_CONTAINER_PATH)
                .withCopyFileToContainer(MountableFile.forHostPath(serverCertPem), SERVER_CERT_CONTAINER_PATH)
                .withCopyFileToContainer(MountableFile.forHostPath(serverKeyPem), SERVER_KEY_CONTAINER_PATH);
        if (serverTrustStorePem != null) {
            container.withCopyFileToContainer(MountableFile.forHostPath(serverTrustStorePem), SERVER_TRUSTSTORE_CONTAINER_PATH);
        }
        container.withExposedPorts(9042);
        container.start();

        // Trusted material to reach this container at all (there's no plaintext fallback) - always the
        // real server/client identities the server is actually configured with, independent of whichever
        // trust/key material the test scenario that triggered this startup will use for its own connection.
        final SslKeyStoreCredentials bootstrapTrustStore = writeTrustStore(serverCertificate, "server");
        final SslKeyStoreCredentials bootstrapKeyStore = requireClientAuth
                ? writeKeyStore(trustedClientKeyPair.getPrivate(), trustedClientCertificate, "client")
                : null;
        createTestKeyspaceOverTls(container, bootstrapTrustStore, bootstrapKeyStore);

        final String contactPoint = container.getContainerIpAddress() + ":" + container.getMappedPort(9042);
        return new RunningSslContainer(contactPoint, container::stop);
    }

    /**
     * Creates the {@code testspace} keyspace {@code verify()}'s "Verify Keyspace" step expects, over a raw
     * driver session using the same TLS material the server is actually configured to trust - the only way
     * to reach this ScyllaDB container at all once {@code client_encryption_options.enabled: true} is set,
     * since (unlike Cassandra) there's no "optional" plaintext fallback.
     */
    private void createTestKeyspaceOverTls(final ScyllaDBContainer container, final SslKeyStoreCredentials trustStore, final SslKeyStoreCredentials keyStore) throws Exception {
        final SSLContext sslContext = buildSslContext(trustStore, keyStore);
        try (CqlSession bootstrapSession = CqlSession.builder()
                .addContactPoint(container.getContactPoint())
                .withLocalDatacenter(LOCAL_DATACENTER)
                .withSslContext(sslContext)
                .withConfigLoader(ScyllaDdlTimeouts.longSchemaTimeoutConfigLoader())
                .build()) {
            CqlDdl.executeWithRetry(bootstrapSession, "create keyspace if not exists " + KEYSPACE
                    + " with replication = { 'class': 'NetworkTopologyStrategy', '" + LOCAL_DATACENTER + "': 1};");
        }
    }

    private static SSLContext buildSslContext(final SslKeyStoreCredentials trustStore, final SslKeyStoreCredentials keyStore) throws Exception {
        final KeyStore trustKeyStore = KeyStore.getInstance(STORE_TYPE);
        try (InputStream inputStream = Files.newInputStream(trustStore.path())) {
            trustKeyStore.load(inputStream, trustStore.password().toCharArray());
        }
        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustKeyStore);

        final KeyManager[] keyManagers;
        if (keyStore == null) {
            keyManagers = null;
        } else {
            final KeyStore clientKeyStore = KeyStore.getInstance(STORE_TYPE);
            try (InputStream inputStream = Files.newInputStream(keyStore.path())) {
                clientKeyStore.load(inputStream, keyStore.password().toCharArray());
            }
            final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(clientKeyStore, keyStore.password().toCharArray());
            keyManagers = keyManagerFactory.getKeyManagers();
        }

        final SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagers, trustManagerFactory.getTrustManagers(), null);
        return sslContext;
    }

    private Path createTargetDirectory(final String testName) throws IOException {
        final Path directory = Paths.get("target", "scylla-ssl-it", testName + "-" + UUID.randomUUID());
        Files.createDirectories(directory);
        return directory;
    }

    /**
     * Builds the server's {@code scylla.yaml} by appending a {@code client_encryption_options} block - built
     * entirely from the fixed container paths the generated PEM files are copied to above - onto the checked-in
     * base template, and writes the assembled result into {@code targetDirectory} for {@code withCopyFileToContainer}
     * to pick up. Nothing here is a static, hard-coded resource: the block itself, not just values within it, is
     * constructed per test.
     */
    private Path buildServerYaml(final Path targetDirectory, final boolean requireClientAuth) throws IOException {
        final String baseYaml;
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(BASE_CONFIG_RESOURCE)) {
            if (inputStream == null) {
                throw new IOException("Classpath resource not found: " + BASE_CONFIG_RESOURCE);
            }
            baseYaml = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }

        final StringBuilder clientEncryptionOptions = new StringBuilder();
        clientEncryptionOptions.append("client_encryption_options:\n");
        clientEncryptionOptions.append("  enabled: true\n");
        clientEncryptionOptions.append("  certificate: ").append(SERVER_CERT_CONTAINER_PATH).append('\n');
        clientEncryptionOptions.append("  keyfile: ").append(SERVER_KEY_CONTAINER_PATH).append('\n');
        clientEncryptionOptions.append("  require_client_auth: ").append(requireClientAuth).append('\n');
        if (requireClientAuth) {
            clientEncryptionOptions.append("  truststore: ").append(SERVER_TRUSTSTORE_CONTAINER_PATH).append('\n');
        }

        final Path yamlFile = targetDirectory.resolve("scylla.yaml");
        Files.writeString(yamlFile, baseYaml + "\n" + clientEncryptionOptions);
        return yamlFile;
    }

    private static Path writeCertificatePem(final X509Certificate certificate, final Path targetDirectory, final String fileName) throws Exception {
        final String encoded = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8)).encodeToString(certificate.getEncoded());
        final Path path = targetDirectory.resolve(fileName);
        Files.writeString(path, String.format(CERTIFICATE_PEM_FORMAT, encoded));
        return path;
    }

    private static Path writePrivateKeyPem(final PrivateKey privateKey, final Path targetDirectory, final String fileName) throws IOException {
        final String encoded = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8)).encodeToString(privateKey.getEncoded());
        final Path path = targetDirectory.resolve(fileName);
        Files.writeString(path, String.format(PRIVATE_KEY_PEM_FORMAT, encoded));
        return path;
    }
}
