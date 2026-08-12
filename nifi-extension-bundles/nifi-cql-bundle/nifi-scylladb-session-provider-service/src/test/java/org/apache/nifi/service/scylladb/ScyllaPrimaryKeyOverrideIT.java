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
import org.apache.nifi.service.cql.it.AbstractCqlPrimaryKeyOverrideIT;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * Primary key RecordPath override coverage from {@link AbstractCqlPrimaryKeyOverrideIT} run against a real
 * ScyllaDB container. Single fixed image version, not parameterized - see {@link ScyllaCrudIT}'s javadoc for
 * why.
 *
 * <p>The container itself belongs to {@link SharedScyllaCluster}, not to this class. It owns the
 * {@code pk_override} keyspace, in which the shared superclass creates three tables before issuing 500+
 * individual inserts and point lookups; those go through the session provider's own connection, while the
 * table creation uses the shared session's raised DDL timeouts.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScyllaPrimaryKeyOverrideIT extends AbstractCqlPrimaryKeyOverrideIT {

    private static final String KEYSPACE = "pk_override";

    @BeforeAll
    void attachToCluster() throws Exception {
        final SharedScyllaCluster cluster = SharedScyllaCluster.getInstance();
        cluster.createKeyspace(KEYSPACE);

        initializeSessionProvider(cluster.connectionInfo(KEYSPACE));
    }

    @Override
    protected CQLExecutionService newSessionProvider() {
        return new ScyllaDBCQLExecutionService();
    }
}
