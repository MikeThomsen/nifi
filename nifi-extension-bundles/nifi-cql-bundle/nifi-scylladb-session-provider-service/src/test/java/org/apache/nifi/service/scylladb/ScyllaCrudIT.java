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
import org.apache.nifi.service.cql.it.AbstractCqlCrudIT;
import org.apache.nifi.service.cql.it.CqlDdl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * CRUD coverage from {@link AbstractCqlCrudIT} run against a real ScyllaDB container. Unlike the Cassandra
 * side, this runs against a single, fixed image version rather than a parameterized matrix.
 *
 * <p>The container itself belongs to {@link SharedScyllaCluster}, not to this class - see there for what
 * sharing one costs and requires. Schema bootstrap stays here rather than moving into the shared holder,
 * because these tables belong to this suite: {@link ScyllaConnectionVerificationIT} shares the
 * {@code testspace} keyspace but never touches a table in it. The statements are idempotent, since which
 * suite reaches the shared container first is not fixed. They are issued inline rather than through an init
 * script because ScyllaDB's Testcontainers image has no {@code withInitScript} equivalent.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScyllaCrudIT extends AbstractCqlCrudIT {

    private static final String KEYSPACE = "testspace";

    @BeforeAll
    void attachToCluster() throws Exception {
        final SharedScyllaCluster cluster = SharedScyllaCluster.getInstance();
        cluster.createKeyspace(KEYSPACE);

        final CqlSession session = cluster.session();
        CqlDdl.executeWithRetry(session, """
                create table if not exists testspace.message
                (
                    sender    text,
                    receiver  text,
                    message   text,
                    when_sent timestamp,
                    primary key ( sender, receiver, when_sent )
                );
                """);
        CqlDdl.executeWithRetry(session, """
                create table if not exists testspace.query_test
                (
                    column_a text,
                    column_b text,
                    when     timestamp,
                    primary key ( (column_a), column_b)
                );
                """);
        CqlDdl.executeWithRetry(session, """
                create table if not exists testspace.counter_test
                (
                    column_a        text,
                    increment_field counter,
                    primary key ( column_a )
                );
                """);
        CqlDdl.executeWithRetry(session, """
                create table if not exists testspace.simple_set_test
                (
                    username text,
                    is_active boolean,
                    primary key ( username )
                );""");

        initializeSessionProvider(cluster.connectionInfo(KEYSPACE));
    }

    @Override
    protected CQLExecutionService newSessionProvider() {
        return new ScyllaDBCQLExecutionService();
    }
}
