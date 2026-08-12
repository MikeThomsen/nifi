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
import org.apache.nifi.service.cql.it.CqlConnectionInfo;
import org.apache.nifi.service.cql.it.CqlDdl;
import org.apache.nifi.service.cql.it.DockerUtils;
import org.testcontainers.cassandra.CassandraContainer;

import java.util.HashMap;
import java.util.Map;

/**
 * One Cassandra container per major version, shared by every IT in this package that only needs a plain,
 * default-configured server.
 *
 * <p>Those suites - CRUD, connection verification, record field types, primary key overrides - used to boot a
 * container each, so the default single-version run paid four container starts to test four things against
 * what was, every time, an identically configured server. Cassandra takes far longer to start than any of
 * these suites take to run, so that startup cost dominated the integration run.
 *
 * <h2>Why keyed by version rather than a plain singleton</h2>
 * {@code CassandraCrudIT} and {@code CassandraConnectionVerificationIT} are {@code @ParameterizedClass} over
 * {@link CassandraTestVersions}, while {@code CassandraRecordFieldTypeIT} and
 * {@code CassandraPrimaryKeyOverrideIT} are pinned to a single version because what they test does not vary
 * release to release. Keying on the version string satisfies both: the pinned suites ask for
 * {@value #PINNED_VERSION} and land on the same instance the parameterized suites use for their
 * {@value #PINNED_VERSION} invocation. With the default single-version matrix that is one container for all
 * four suites; with {@code -DTEST_CASSANDRA_OLDER_VERSIONS=true} it is one per version rather than one per
 * suite per version.
 *
 * <h2>Isolation between suites</h2>
 * Sharing makes keyspace separation load-bearing rather than incidental: {@code testspace} (created by
 * {@code init.cql} at container start) belongs to the CRUD and verification suites, and the other two own
 * {@code type_coverage} and {@code pk_override}, which they create through {@link #createKeyspace}. A suite
 * must not read or write outside its own keyspace, and any suite asserting on an exact row or counter value
 * must be the only writer of that table.
 *
 * <h2>Lifetime</h2>
 * Deliberately never stopped. Testcontainers' Ryuk sidecar removes containers when the JVM that created them
 * exits, which is the documented way to share one - an explicit {@code stop()} from any one suite's
 * {@code @AfterAll} would tear the container out from under the suites that had not run yet. Note the
 * trade-off this implies for a multi-version run: every version's container stays up once touched, so peak
 * memory is the sum rather than one at a time.
 */
final class SharedCassandraCluster {

    /**
     * The version the suites that do not vary by release pin to. Taken from {@link CassandraTestVersions}
     * rather than declared here, so that bumping the current major cannot silently leave those suites
     * starting a container of their own for a version nothing else asks for.
     */
    static final String PINNED_VERSION = CassandraTestVersions.CURRENT_VERSION;

    private static final String DATACENTER = "datacenter1";

    private static final int CQL_PORT = 9042;

    private static final Map<String, SharedCassandraCluster> BY_VERSION = new HashMap<>();

    private final String contactPoint;

    private final CqlSession session;

    private SharedCassandraCluster(final String contactPoint, final CqlSession session) {
        this.contactPoint = contactPoint;
        this.session = session;
    }

    /**
     * The cluster for {@code version}, starting it on first request and reusing it thereafter. Synchronized
     * because the map operation spans a container start; test classes run sequentially today, so this is
     * cheap insurance rather than a hot path.
     */
    static synchronized SharedCassandraCluster forVersion(final String version) {
        return BY_VERSION.computeIfAbsent(version, SharedCassandraCluster::start);
    }

    private static SharedCassandraCluster start(final String version) {
        // init.cql creates the "testspace" keyspace and its tables, which the CRUD and connection
        // verification suites both expect to exist before their first test.
        final CassandraContainer container = new CassandraContainer("cassandra:" + version)
                .withTmpFs(Map.of("/var/lib/cassandra", "rw,size=1g"))
                .withCreateContainerCmdModifier(DockerUtils.createMemoryLimits(2L, 2))
                .withInitScript("init.cql");
        container.withExposedPorts(CQL_PORT);
        container.start();

        final CqlSession session = CqlSession.builder()
                .addContactPoint(container.getContactPoint())
                .withLocalDatacenter(DATACENTER)
                .build();

        return new SharedCassandraCluster(
                container.getContainerIpAddress() + ":" + container.getMappedPort(CQL_PORT), session);
    }

    /**
     * Creates {@code keyspace} if it is not already there, for a suite that owns a keyspace of its own rather
     * than the one {@code init.cql} sets up. Idempotent because the container outlives whichever suite got
     * there first.
     */
    void createKeyspace(final String keyspace) {
        CqlDdl.executeWithRetry(session, "create keyspace if not exists " + keyspace
                + " with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    }

    CqlConnectionInfo connectionInfo(final String keyspace) {
        return new CqlConnectionInfo(contactPoint, DATACENTER, keyspace, session);
    }

    CqlSession session() {
        return session;
    }
}
