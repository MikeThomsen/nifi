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
package org.apache.nifi.service.cql;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Runs {@link AbstractCQLDistributedMapCacheIT}'s shared coverage against a real Cassandra container.
 * Only runs when the {@code cassandra} Maven profile is active (the default for this module) -- see the
 * {@code it.backend} system property wired up in this module's pom.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfSystemProperty(named = "it.backend", matches = "cassandra")
class CQLDistributedMapCacheCassandraIT extends AbstractCQLDistributedMapCacheIT {

    private static final String IMAGE = "cassandra:5.0";

    private CassandraContainer container;

    @BeforeAll
    void startContainer() {
        container = new CassandraContainer(IMAGE);
        container.withExposedPorts(9042);
        container.start();

        contactPoint = container.getContainerIpAddress() + ":" + container.getMappedPort(9042);
        bootstrapSchema(contactPoint);
    }

    @AfterAll
    void stopContainer() {
        container.stop();
    }
}
