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
package org.apache.nifi.service.cql.it;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import javax.security.auth.x500.X500Principal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Exercises the {@code SSL Context Service} property of any {@link CQLExecutionService} implementation
 * against a real, differently-provisioned-per-backend container, for one-way (server-authenticated only) and
 * two-way (mutual) TLS, including that an untrusted certificate reliably fails rather than silently
 * succeeding. All client-side certificate/keystore generation and the actual NiFi-side configuration and
 * {@code verify()} assertion are identical across backends and live entirely here; only how each backend's
 * server gets its own TLS material (Java keystore vs PEM files, for example) and how the test keyspace gets
 * created once TLS is enabled differs, which is why {@link #startOneWayServer} and {@link #startTwoWayServer}
 * are the only two abstract hooks.
 * <p>
 * Both hooks are called for the corresponding success AND failure test - the server is always configured
 * with the real, correct certificate/key material passed in; only what the client under test later chooses
 * to trust/present (handled entirely in this class) determines whether a given test expects success or
 * failure. Concrete implementations may use the always-correct material to bootstrap the test keyspace
 * (needed where there's no plaintext fallback) regardless of which scenario invoked them.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractCqlSslIT {

    protected static final String KEYSPACE = "testspace";

    protected static final String LOCAL_DATACENTER = "datacenter1";

    protected static final String STORE_TYPE = "PKCS12";

    private static final Path SSL_ARTIFACT_DIRECTORY = createSslArtifactDirectory();

    /**
     * @return a fresh, unconfigured instance of the backend implementation under test
     */
    protected abstract CQLExecutionService newSessionProvider();

    /**
     * Starts a container with one-way TLS enabled using the given already-generated server certificate/key,
     * and returns its contact point once the test keyspace exists and the server is ready for the real test
     * connection this method's caller will make.
     */
    protected abstract RunningSslContainer startOneWayServer(KeyPair serverKeyPair, X509Certificate serverCertificate) throws Exception;

    /**
     * Starts a container with two-way (mutual) TLS enabled using the given already-generated server
     * certificate/key, configured to require and trust {@code trustedClientCertificate}. The full client key
     * pair (not just the certificate) is supplied so a backend that has no plaintext fallback can build its
     * own bootstrap client identity to create the test keyspace over TLS.
     */
    protected abstract RunningSslContainer startTwoWayServer(KeyPair serverKeyPair, X509Certificate serverCertificate,
                                                              KeyPair trustedClientKeyPair, X509Certificate trustedClientCertificate) throws Exception;

    /**
     * A running container's connection coordinates plus how to stop it, returned by the abstract
     * {@code start*Server} hooks so the shared test methods never need to know the concrete container type.
     */
    public record RunningSslContainer(String contactPoint, Runnable shutdown) {
    }

    /**
     * A generated PKCS12 file together with the random password it was protected with, so callers never need
     * to thread the password through separately or guess it back from the file. Reusable by concrete
     * subclasses for the server's own keystore too, where the backend's server-side TLS is also Java-keystore-based.
     */
    public record SslKeyStoreCredentials(Path path, String password) {
    }

    @Test
    @DisplayName("A CQL execution service configured with only a truststore establishes a one-way TLS connection")
    void testOneWaySslConnectionSucceeds() throws Exception {
        final KeyPair serverKeyPair = generateKeyPair();
        final X509Certificate serverCertificate = buildSelfSignedCertificate(serverKeyPair, "CN=cql-ssl-server");

        final RunningSslContainer container = startOneWayServer(serverKeyPair, serverCertificate);
        try {
            // Trusts the server certificate directly (no CA hierarchy involved) - sufficient for one-way TLS,
            // where the client only needs to verify the identity of the server it's connecting to.
            final SslKeyStoreCredentials clientTrustStore = writeTrustStore(serverCertificate, "server");
            assertSslConnectionOutcome(container.contactPoint(), clientTrustStore, null, ConfigVerificationResult.Outcome.SUCCESSFUL);
        } finally {
            container.shutdown().run();
        }
    }

    @Test
    @DisplayName("A CQL execution service that trusts the wrong certificate fails a one-way TLS connection")
    void testOneWaySslConnectionFailsWithUntrustedServerCertificate() throws Exception {
        final KeyPair serverKeyPair = generateKeyPair();
        final X509Certificate serverCertificate = buildSelfSignedCertificate(serverKeyPair, "CN=cql-ssl-server");

        final RunningSslContainer container = startOneWayServer(serverKeyPair, serverCertificate);
        try {
            // An unrelated certificate the server never presents - simulates a client with the wrong trust anchor.
            final KeyPair untrustedKeyPair = generateKeyPair();
            final X509Certificate untrustedCertificate = buildSelfSignedCertificate(untrustedKeyPair, "CN=untrusted-server");
            final SslKeyStoreCredentials wrongTrustStore = writeTrustStore(untrustedCertificate, "untrusted");

            assertSslConnectionOutcome(container.contactPoint(), wrongTrustStore, null, ConfigVerificationResult.Outcome.FAILED);
        } finally {
            container.shutdown().run();
        }
    }

    @Test
    @DisplayName("A CQL execution service configured with a keystore and truststore establishes a two-way (mutual) TLS connection")
    void testTwoWaySslConnectionSucceeds() throws Exception {
        final KeyPair serverKeyPair = generateKeyPair();
        final X509Certificate serverCertificate = buildSelfSignedCertificate(serverKeyPair, "CN=cql-ssl-server");

        final KeyPair clientKeyPair = generateKeyPair();
        final X509Certificate clientCertificate = buildSelfSignedCertificate(clientKeyPair, "CN=cql-execution-service-client");

        final RunningSslContainer container = startTwoWayServer(serverKeyPair, serverCertificate, clientKeyPair, clientCertificate);
        try {
            final SslKeyStoreCredentials clientTrustStore = writeTrustStore(serverCertificate, "server");
            final SslKeyStoreCredentials clientKeyStore = writeKeyStore(clientKeyPair.getPrivate(), clientCertificate, "client");

            assertSslConnectionOutcome(container.contactPoint(), clientTrustStore, clientKeyStore, ConfigVerificationResult.Outcome.SUCCESSFUL);
        } finally {
            container.shutdown().run();
        }
    }

    @Test
    @DisplayName("A CQL execution service presenting a certificate the server doesn't trust fails a two-way (mutual) TLS connection")
    void testTwoWaySslConnectionFailsWithUntrustedClientCertificate() throws Exception {
        final KeyPair serverKeyPair = generateKeyPair();
        final X509Certificate serverCertificate = buildSelfSignedCertificate(serverKeyPair, "CN=cql-ssl-server");

        final KeyPair trustedClientKeyPair = generateKeyPair();
        final X509Certificate trustedClientCertificate = buildSelfSignedCertificate(trustedClientKeyPair, "CN=cql-execution-service-client");

        final RunningSslContainer container = startTwoWayServer(serverKeyPair, serverCertificate, trustedClientKeyPair, trustedClientCertificate);
        try {
            final SslKeyStoreCredentials clientTrustStore = writeTrustStore(serverCertificate, "server");

            // A different client identity than the one the server's truststore actually trusts.
            final KeyPair rogueClientKeyPair = generateKeyPair();
            final X509Certificate rogueClientCertificate = buildSelfSignedCertificate(rogueClientKeyPair, "CN=untrusted-client");
            final SslKeyStoreCredentials rogueClientKeyStore = writeKeyStore(rogueClientKeyPair.getPrivate(), rogueClientCertificate, "client");

            assertSslConnectionOutcome(container.contactPoint(), clientTrustStore, rogueClientKeyStore, ConfigVerificationResult.Outcome.FAILED);
        } finally {
            container.shutdown().run();
        }
    }

    private void assertSslConnectionOutcome(final String contactPoint, final SslKeyStoreCredentials trustStore, final SslKeyStoreCredentials keyStore,
                                             final ConfigVerificationResult.Outcome expectedOutcome) throws Exception {
        final StandardSSLContextService sslContextService = new StandardSSLContextService();
        final CQLExecutionService sessionProvider = newSessionProvider();
        final TestRunner runner = TestRunners.newTestRunner(new MockCqlProcessor());

        runner.addControllerService("ssl-context-service", sslContextService);
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, trustStore.path().toString());
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, trustStore.password());
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, STORE_TYPE);
        if (keyStore != null) {
            runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, keyStore.path().toString());
            runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, keyStore.password());
            runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, STORE_TYPE);
        }
        runner.enableControllerService(sslContextService);

        runner.addControllerService("cql-session-provider", sessionProvider);
        runner.setProperty(sessionProvider, CQLExecutionService.CONTACT_POINTS, contactPoint);
        runner.setProperty(sessionProvider, CQLExecutionService.DATACENTER, LOCAL_DATACENTER);
        runner.setProperty(sessionProvider, CQLExecutionService.KEYSPACE, KEYSPACE);
        runner.setProperty(sessionProvider, CQLExecutionService.PROP_SSL_CONTEXT_SERVICE, "ssl-context-service");

        final List<ConfigVerificationResult> results = runner.verify(sessionProvider, Collections.emptyMap());

        assertFalse(results.isEmpty());
        if (expectedOutcome == ConfigVerificationResult.Outcome.SUCCESSFUL) {
            for (final ConfigVerificationResult result : results) {
                assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, result.getOutcome(), result.getExplanation());
            }
        } else {
            final ConfigVerificationResult connectionResult = results.stream()
                    .filter(result -> "Establish Connection".equals(result.getVerificationStepName()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected an 'Establish Connection' verification result"));
            assertEquals(expectedOutcome, connectionResult.getOutcome());
        }
    }

    protected static KeyPair generateKeyPair() throws Exception {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);
        return keyPairGenerator.generateKeyPair();
    }

    protected static X509Certificate buildSelfSignedCertificate(final KeyPair keyPair, final String distinguishedName) {
        return new StandardCertificateBuilder(keyPair, new X500Principal(distinguishedName), Duration.ofDays(1))
                .setDnsSubjectAlternativeNames(List.of("localhost"))
                .build();
    }

    protected static SslKeyStoreCredentials writeKeyStore(final PrivateKey privateKey, final X509Certificate certificate, final String alias) throws Exception {
        final String password = generateStorePassword();
        final KeyStore keyStore = KeyStore.getInstance(STORE_TYPE);
        keyStore.load(null);
        keyStore.setKeyEntry(alias, privateKey, password.toCharArray(), new X509Certificate[]{certificate});

        final Path path = Files.createTempFile(SSL_ARTIFACT_DIRECTORY, alias + "-keystore", ".p12");
        try (OutputStream outputStream = Files.newOutputStream(path)) {
            keyStore.store(outputStream, password.toCharArray());
        }
        return new SslKeyStoreCredentials(path, password);
    }

    protected static SslKeyStoreCredentials writeTrustStore(final X509Certificate certificate, final String alias) throws Exception {
        final String password = generateStorePassword();
        final KeyStore trustStore = KeyStore.getInstance(STORE_TYPE);
        trustStore.load(null);
        trustStore.setCertificateEntry(alias, certificate);

        final Path path = Files.createTempFile(SSL_ARTIFACT_DIRECTORY, alias + "-truststore", ".p12");
        try (OutputStream outputStream = Files.newOutputStream(path)) {
            trustStore.store(outputStream, password.toCharArray());
        }
        return new SslKeyStoreCredentials(path, password);
    }

    private static String generateStorePassword() {
        final SecureRandom secureRandom = new SecureRandom();
        final byte[] bytes = new byte[24];
        secureRandom.nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }

    // Written into the current module's own target/ directory (resolved relative to the test JVM's working
    // directory, which Surefire/Failsafe set to the module root) rather than the OS temp directory, so
    // generated stores are cleaned up by "mvn clean" like any other build artifact instead of relying on
    // deleteOnExit().
    private static Path createSslArtifactDirectory() {
        try {
            return Files.createDirectories(Path.of("target", "cql-ssl-it"));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
