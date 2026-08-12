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
import org.apache.nifi.service.cql.it.AbstractCqlRecordFieldTypeIT;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * Type-coverage tests from {@link AbstractCqlRecordFieldTypeIT} run against a real ScyllaDB container.
 * Single fixed image version, not parameterized - see {@link ScyllaCrudIT}'s javadoc for why.
 *
 * <p>The container itself belongs to {@link SharedScyllaCluster}, not to this class. It owns the
 * {@code type_coverage} keyspace, in which it creates a table per type under test - a rapid sequence of
 * roughly 40 schema-modifying statements, which is why the shared session is configured with
 * {@link ScyllaDdlTimeouts}' raised ceilings and every statement goes through the retrying DDL helper.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScyllaRecordFieldTypeIT extends AbstractCqlRecordFieldTypeIT {

    private static final String KEYSPACE = "type_coverage";

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
