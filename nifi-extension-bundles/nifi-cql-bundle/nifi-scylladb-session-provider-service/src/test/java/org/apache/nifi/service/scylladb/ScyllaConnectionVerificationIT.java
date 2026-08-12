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

import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.service.cql.it.AbstractCqlConnectionVerificationIT;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * Connection/{@code verify()} coverage from {@link AbstractCqlConnectionVerificationIT} run against a real
 * ScyllaDB container. Single fixed image version, not parameterized - see {@link ScyllaCrudIT}'s javadoc for
 * why.
 *
 * <p>The container itself belongs to {@link SharedScyllaCluster}, not to this class. Nothing here writes to a
 * table - every test verifies a configuration - so sharing costs this suite nothing; it needs only the
 * {@code testspace} keyspace to exist for {@code verify()}'s keyspace check, which it creates idempotently
 * rather than relying on {@link ScyllaCrudIT} having run first.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScyllaConnectionVerificationIT extends AbstractCqlConnectionVerificationIT {

    private static final String KEYSPACE = "testspace";

    @BeforeAll
    void attachToCluster() {
        final SharedScyllaCluster cluster = SharedScyllaCluster.getInstance();
        cluster.createKeyspace(KEYSPACE);

        initializeConnectionInfo(cluster.connectionInfo(KEYSPACE));
    }

    @Override
    protected CQLExecutionService newSessionProvider() {
        return new ScyllaDBCQLExecutionService();
    }
}
