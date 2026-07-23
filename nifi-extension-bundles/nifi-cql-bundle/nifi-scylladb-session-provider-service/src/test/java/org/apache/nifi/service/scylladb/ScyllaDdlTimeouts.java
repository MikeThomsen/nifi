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
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

import java.time.Duration;

/**
 * Shared request/schema-agreement timeout config, and retry-on-timeout DDL execution, for bootstrap sessions
 * that issue DDL against a real ScyllaDB container (every IT class in this package that creates a keyspace,
 * table, type, or role directly). Schema-modifying statements settle through ScyllaDB's Raft-based schema
 * management (group0), which the driver's 2 second defaults for request and internal schema-refresh/agreement
 * timeouts aren't reliably enough for, and which can occasionally exceed even a generous timeout under host
 * contention - hence the retry on top of the raised ceiling.
 */
final class ScyllaDdlTimeouts {

    /**
     * System property overriding the DDL/schema-agreement timeout ceiling (in seconds) used by the
     * bootstrap sessions in this package, for environments where the default isn't enough (e.g. a
     * contended CI host, or a local machine already running other Docker-heavy test suites).
     */
    static final String DDL_TIMEOUT_PROPERTY = "TEST_SCYLLA_DDL_TIMEOUT_SECONDS";

    private static final int DEFAULT_DDL_TIMEOUT_SECONDS = 30;

    private static final int DDL_MAX_ATTEMPTS = 3;

    private ScyllaDdlTimeouts() {
    }

    static DriverConfigLoader longSchemaTimeoutConfigLoader() {
        final Duration timeout = Duration.ofSeconds(Integer.getInteger(DDL_TIMEOUT_PROPERTY, DEFAULT_DDL_TIMEOUT_SECONDS));
        return DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, timeout)
                .withDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, timeout)
                .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, timeout)
                .withDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT, timeout)
                .build();
    }

    /**
     * Executes a schema-modifying statement, retrying on {@link DriverTimeoutException}. A DDL statement can
     * legitimately succeed server-side after the client already gave up waiting for schema agreement, so
     * every statement passed here must be idempotent ("if not exists") - otherwise a retry after a
     * false-negative timeout would fail with "already exists" instead of masking the flake.
     */
    static void executeDdlWithRetry(final CqlSession session, final String cql) {
        DriverTimeoutException lastFailure = null;
        for (int attempt = 1; attempt <= DDL_MAX_ATTEMPTS; attempt++) {
            try {
                session.execute(cql);
                return;
            } catch (final DriverTimeoutException e) {
                lastFailure = e;
            }
        }
        throw lastFailure;
    }
}
