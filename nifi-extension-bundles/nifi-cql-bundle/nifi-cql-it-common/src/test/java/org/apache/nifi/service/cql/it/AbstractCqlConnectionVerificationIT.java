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
import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises connection establishment and configuration verification ({@code verify()}, including the
 * {@code Driver Configuration File} property) on any {@link CQLExecutionService} implementation, against
 * whichever backend the concrete subclass wires up. Never touches a table - data-manipulation coverage lives
 * in {@link AbstractCqlCrudIT}.
 * <p>
 * Setup is a plain protected method rather than a JUnit lifecycle callback for the same reason documented on
 * {@link AbstractCqlCrudIT}: on a {@code @ParameterizedClass} leaf, {@code @BeforeAll} - even inherited from
 * this superclass - runs before the leaf's own {@code @BeforeParameterizedClassInvocation} method.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractCqlConnectionVerificationIT {

    private CqlConnectionInfo connectionInfo;

    /**
     * @return a fresh, unconfigured instance of the backend implementation under test
     */
    protected abstract CQLExecutionService newSessionProvider();

    protected void initializeConnectionInfo(final CqlConnectionInfo connectionInfo) {
        this.connectionInfo = connectionInfo;
    }

    @Test
    @DisplayName("Verifying a fully valid configuration reports success for every verification step")
    void testVerifySucceedsWithValidConfiguration() throws Exception {
        final CQLExecutionService sessionProvider = newSessionProvider();
        final TestRunner runner = TestRunners.newTestRunner(new MockCqlProcessor());

        runner.addControllerService("cql-session-provider", sessionProvider);
        runner.setProperty(sessionProvider, CQLExecutionService.CONTACT_POINTS, connectionInfo.contactPoint());
        runner.setProperty(sessionProvider, CQLExecutionService.DATACENTER, connectionInfo.datacenter());
        runner.setProperty(sessionProvider, CQLExecutionService.KEYSPACE, connectionInfo.keyspace());

        final List<ConfigVerificationResult> results = runner.verify(sessionProvider, Collections.emptyMap());

        assertFalse(results.isEmpty());
        for (final ConfigVerificationResult result : results) {
            assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, result.getOutcome(), result.getExplanation());
        }

        assertTrue(results.stream().anyMatch(result -> "Establish Connection".equals(result.getVerificationStepName())));
        assertTrue(results.stream().anyMatch(result -> "Verify Datacenter".equals(result.getVerificationStepName())));
        assertTrue(results.stream().anyMatch(result -> "Verify Keyspace".equals(result.getVerificationStepName())));
    }

    @Test
    @DisplayName("Verifying a configuration with a nonexistent datacenter reports a failed datacenter check")
    void testVerifyFailsWithInvalidDatacenter() throws Exception {
        final CQLExecutionService sessionProvider = newSessionProvider();
        final TestRunner runner = TestRunners.newTestRunner(new MockCqlProcessor());

        runner.addControllerService("cql-session-provider", sessionProvider);
        runner.setProperty(sessionProvider, CQLExecutionService.CONTACT_POINTS, connectionInfo.contactPoint());
        runner.setProperty(sessionProvider, CQLExecutionService.DATACENTER, "not-a-real-datacenter");
        runner.setProperty(sessionProvider, CQLExecutionService.KEYSPACE, connectionInfo.keyspace());

        final List<ConfigVerificationResult> results = runner.verify(sessionProvider, Collections.emptyMap());

        final ConfigVerificationResult connectionResult = results.stream()
                .filter(result -> "Verify Datacenter".equals(result.getVerificationStepName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected a 'Verify Datacenter' verification result"));

        assertEquals(ConfigVerificationResult.Outcome.FAILED, connectionResult.getOutcome());
    }

    @Test
    @DisplayName("Verifying a configuration with a syntactically valid driver configuration file reports success")
    void testVerifySucceedsWithValidDriverConfigurationFile() throws Exception {
        final Path configFile = writeDriverConfigFile("""
                datastax-java-driver {
                  basic.request.timeout = 15 seconds
                }
                """);

        final CQLExecutionService sessionProvider = newSessionProvider();
        final TestRunner runner = TestRunners.newTestRunner(new MockCqlProcessor());

        runner.addControllerService("cql-session-provider", sessionProvider);
        runner.setProperty(sessionProvider, CQLExecutionService.CONTACT_POINTS, connectionInfo.contactPoint());
        runner.setProperty(sessionProvider, CQLExecutionService.DATACENTER, connectionInfo.datacenter());
        runner.setProperty(sessionProvider, CQLExecutionService.KEYSPACE, connectionInfo.keyspace());
        runner.setProperty(sessionProvider, CQLExecutionService.DRIVER_CONFIGURATION_FILE, configFile.toString());

        final List<ConfigVerificationResult> results = runner.verify(sessionProvider, Collections.emptyMap());

        assertFalse(results.isEmpty());
        for (final ConfigVerificationResult result : results) {
            assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, result.getOutcome(), result.getExplanation());
        }
    }

    @Test
    @DisplayName("Verifying a configuration with a malformed driver configuration file reports a failed connection")
    void testVerifyFailsWithSyntacticallyInvalidDriverConfigurationFile() throws Exception {
        // Missing the closing brace for the "datastax-java-driver" block makes this invalid HOCON.
        final Path configFile = writeDriverConfigFile("""
                datastax-java-driver {
                  basic.request.timeout = 15 seconds
                """);

        final CQLExecutionService sessionProvider = newSessionProvider();
        final TestRunner runner = TestRunners.newTestRunner(new MockCqlProcessor());

        runner.addControllerService("cql-session-provider", sessionProvider);
        runner.setProperty(sessionProvider, CQLExecutionService.CONTACT_POINTS, connectionInfo.contactPoint());
        runner.setProperty(sessionProvider, CQLExecutionService.DATACENTER, connectionInfo.datacenter());
        runner.setProperty(sessionProvider, CQLExecutionService.KEYSPACE, connectionInfo.keyspace());
        runner.setProperty(sessionProvider, CQLExecutionService.DRIVER_CONFIGURATION_FILE, configFile.toString());

        final List<ConfigVerificationResult> results = runner.verify(sessionProvider, Collections.emptyMap());

        final ConfigVerificationResult connectionResult = results.stream()
                .filter(result -> "Establish Connection".equals(result.getVerificationStepName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected an 'Establish Connection' verification result"));

        assertEquals(ConfigVerificationResult.Outcome.FAILED, connectionResult.getOutcome());
    }

    @Test
    @DisplayName("Verifying a configuration whose driver configuration file points at the wrong port reports a failed connection")
    void testVerifyFailsWithDriverConfigurationFileUsingWrongPort() throws Exception {
        // A port next to the real contact point's port that Testcontainers never exposed, so nothing on the
        // container host is listening on it.
        final String[] hostAndPort = connectionInfo.contactPoint().split(":");
        final String host = hostAndPort[0];
        final int wrongPort = Integer.parseInt(hostAndPort[1]) + 1;

        final Path configFile = writeDriverConfigFile(String.format("""
                datastax-java-driver {
                  basic.contact-points = ["%s:%d"]
                }
                """, host, wrongPort));

        final CQLExecutionService sessionProvider = newSessionProvider();
        final TestRunner runner = TestRunners.newTestRunner(new MockCqlProcessor());

        runner.addControllerService("cql-session-provider", sessionProvider);
        runner.setProperty(sessionProvider, CQLExecutionService.DATACENTER, connectionInfo.datacenter());
        runner.setProperty(sessionProvider, CQLExecutionService.KEYSPACE, connectionInfo.keyspace());
        runner.setProperty(sessionProvider, CQLExecutionService.DRIVER_CONFIGURATION_FILE, configFile.toString());
        // Contact Points is deliberately left unset here: the Java Driver unions the config file's contact
        // points with any supplied programmatically rather than one overriding the other, so leaving this
        // unset guarantees the only contact point in play is the wrong one supplied by the file.

        final List<ConfigVerificationResult> results = runner.verify(sessionProvider, Collections.emptyMap());

        final ConfigVerificationResult connectionResult = results.stream()
                .filter(result -> "Establish Connection".equals(result.getVerificationStepName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected an 'Establish Connection' verification result"));

        assertEquals(ConfigVerificationResult.Outcome.FAILED, connectionResult.getOutcome());
    }

    private static Path writeDriverConfigFile(final String content) throws IOException {
        final Path configFile = Files.createTempFile("CqlDriverConfig", ".conf");
        Files.writeString(configFile, content, StandardCharsets.UTF_8);
        configFile.toFile().deleteOnExit();
        return configFile;
    }
}
