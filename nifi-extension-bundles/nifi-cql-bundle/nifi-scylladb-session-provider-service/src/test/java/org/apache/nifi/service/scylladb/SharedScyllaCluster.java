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
import org.apache.nifi.service.cql.it.CqlConnectionInfo;
import org.apache.nifi.service.cql.it.CqlDdl;
import org.testcontainers.scylladb.ScyllaDBContainer;

/**
 * The single ScyllaDB container shared by every IT in this package that only needs a plain,
 * default-configured server: CRUD, connection verification, record field types, and primary key overrides.
 *
 * <p>Those four used to boot a container each, so an integration run paid four ScyllaDB starts to test four
 * things against what was, every time, an identically configured server. This is the ScyllaDB half of
 * {@link org.apache.nifi.service.cassandra.SharedCassandraCluster}, and simpler than it: ScyllaDB runs
 * against one fixed image here rather than a version matrix, so there is nothing to key on.
 *
 * <p>Suites that cannot share stay as they were - {@code ScyllaAuthenticationIT} and {@code ScyllaSslIT}
 * provision servers whose authenticator and TLS settings are baked into {@code scylla.yaml} at startup and
 * cannot be toggled on a running container.
 *
 * <h2>Isolation between suites</h2>
 * Sharing makes keyspace separation load-bearing rather than incidental: {@code testspace} belongs to the
 * CRUD and verification suites, and the other two own {@code type_coverage} and {@code pk_override}. Every
 * suite creates its own keyspace through {@link #createKeyspace}, idempotently, since which suite runs first
 * is not fixed. A suite must not read or write outside its own keyspace, and any suite asserting on an exact
 * row or counter value must be the only writer of that table.
 *
 * <h2>Lifetime</h2>
 * Deliberately never stopped. Testcontainers' Ryuk sidecar removes containers when the JVM that created them
 * exits, which is the documented way to share one - an explicit {@code stop()} from any one suite's
 * {@code @AfterAll} would tear the container out from under the suites that had not run yet.
 *
 * <h2>Session timeouts</h2>
 * The shared session carries {@link ScyllaDdlTimeouts#longSchemaTimeoutConfigLoader()}, which every one of
 * these suites was already building its own session with: schema statements settle through ScyllaDB's
 * Raft-based schema management, which the driver's 2 second defaults are not reliably enough for.
 */
final class SharedScyllaCluster {

    private static final String IMAGE = "scylladb/scylla:2026.2";

    private static final String DATACENTER = "datacenter1";

    private static final int CQL_PORT = 9042;

    private static SharedScyllaCluster instance;

    private final String contactPoint;

    private final CqlSession session;

    private SharedScyllaCluster(final String contactPoint, final CqlSession session) {
        this.contactPoint = contactPoint;
        this.session = session;
    }

    /**
     * The shared cluster, starting it on first request and reusing it thereafter.
     */
    static synchronized SharedScyllaCluster getInstance() {
        if (instance == null) {
            instance = start();
        }
        return instance;
    }

    private static SharedScyllaCluster start() {
        final ScyllaDBContainer container = new ScyllaDBContainer(IMAGE);
        container.withExposedPorts(CQL_PORT);
        container.start();

        final CqlSession session = CqlSession.builder()
                .addContactPoint(container.getContactPoint())
                .withLocalDatacenter(DATACENTER)
                .withConfigLoader(ScyllaDdlTimeouts.longSchemaTimeoutConfigLoader())
                .build();

        return new SharedScyllaCluster(
                container.getContainerIpAddress() + ":" + container.getMappedPort(CQL_PORT), session);
    }

    /**
     * Creates {@code keyspace} if it is not already there. Idempotent because the container outlives
     * whichever suite got there first.
     */
    void createKeyspace(final String keyspace) {
        CqlDdl.executeWithRetry(session, "create keyspace if not exists " + keyspace
                + " with replication = { 'class': 'NetworkTopologyStrategy', '" + DATACENTER + "': 1};");
    }

    CqlConnectionInfo connectionInfo(final String keyspace) {
        return new CqlConnectionInfo(contactPoint, DATACENTER, keyspace, session);
    }

    CqlSession session() {
        return session;
    }
}
