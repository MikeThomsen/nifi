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

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Exercises real-credential authentication against any {@link CQLExecutionService} implementation, for both
 * a correct and an incorrect password. The concrete subclass is responsible for starting a container with
 * {@code PasswordAuthenticator} (or equivalent) enabled and creating a role named {@code admin} with a real,
 * generated password before calling {@link #initializeAuthConnectionInfo(String, String, String, String)} -
 * server-side auth config can't be toggled on an already-running container, so unlike
 * {@link AbstractCqlConnectionVerificationIT}, each concrete subclass owns a dedicated container rather than
 * sharing the one used for the rest of {@code verify()} coverage.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractCqlAuthenticationIT {

    private static final String ADMIN_ROLE = "admin";

    private String contactPoint;

    private String datacenter;

    private String keyspace;

    private String realPassword;

    /**
     * @return a fresh, unconfigured instance of the backend implementation under test
     */
    protected abstract CQLExecutionService newSessionProvider();

    /**
     * Called by the concrete subclass once its dedicated container is running and the {@code admin} role has
     * been created with {@code realPassword}.
     */
    protected void initializeAuthConnectionInfo(final String contactPoint, final String datacenter, final String keyspace, final String realPassword) {
        this.contactPoint = contactPoint;
        this.datacenter = datacenter;
        this.keyspace = keyspace;
        this.realPassword = realPassword;
    }

    @Test
    @DisplayName("A CQL execution service configured with a real role authenticates successfully against PasswordAuthenticator")
    void testConnectsWithRealUsernameAndPassword() throws Exception {
        final List<ConfigVerificationResult> results = verifyWithPassword(realPassword);

        assertFalse(results.isEmpty());
        for (final ConfigVerificationResult result : results) {
            assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, result.getOutcome(), result.getExplanation());
        }
    }

    @Test
    @DisplayName("A CQL execution service configured with an incorrect password fails to authenticate against PasswordAuthenticator")
    void testFailsWithIncorrectPassword() throws Exception {
        // A password that isn't the one the "admin" role was created with above.
        final List<ConfigVerificationResult> results = verifyWithPassword(UUID.randomUUID().toString());

        final ConfigVerificationResult connectionResult = results.stream()
                .filter(result -> "Establish Connection".equals(result.getVerificationStepName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected an 'Establish Connection' verification result"));

        assertEquals(ConfigVerificationResult.Outcome.FAILED, connectionResult.getOutcome());
    }

    private List<ConfigVerificationResult> verifyWithPassword(final String password) throws Exception {
        final CQLExecutionService sessionProvider = newSessionProvider();
        final TestRunner runner = TestRunners.newTestRunner(new MockCqlProcessor());

        runner.addControllerService("auth-session-provider", sessionProvider);
        runner.setProperty(sessionProvider, CQLExecutionService.USERNAME, ADMIN_ROLE);
        runner.setProperty(sessionProvider, CQLExecutionService.PASSWORD, password);
        runner.setProperty(sessionProvider, CQLExecutionService.CONTACT_POINTS, contactPoint);
        runner.setProperty(sessionProvider, CQLExecutionService.DATACENTER, datacenter);
        runner.setProperty(sessionProvider, CQLExecutionService.KEYSPACE, keyspace);

        return runner.verify(sessionProvider, Collections.emptyMap());
    }
}
