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

import org.apache.nifi.service.cql.api.QueryOverrides;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class CassandraCQLExecutionServiceQueryOverridesTest {

    @Test
    void testResolveFetchSizeUsesDefaultWhenOverridesIsNull() {
        assertEquals(100, CassandraCQLExecutionService.resolveFetchSize(null, 100));
    }

    @Test
    void testResolveFetchSizeUsesDefaultWhenFetchSizeNotOverridden() {
        assertEquals(100, CassandraCQLExecutionService.resolveFetchSize(new QueryOverrides(null, null), 100));
    }

    @Test
    void testResolveFetchSizeUsesOverrideWhenPresent() {
        assertEquals(50, CassandraCQLExecutionService.resolveFetchSize(new QueryOverrides(50, null), 100));
    }

    @Test
    void testResolveTimeoutOverrideIsNullWhenOverridesIsNull() {
        assertNull(CassandraCQLExecutionService.resolveTimeoutOverride(null));
    }

    @Test
    void testResolveTimeoutOverrideIsNullWhenNotOverridden() {
        assertNull(CassandraCQLExecutionService.resolveTimeoutOverride(new QueryOverrides(null, null)));
    }

    @Test
    void testResolveTimeoutOverrideReturnsOverrideWhenPresent() {
        final Duration timeout = Duration.ofSeconds(5);
        assertEquals(timeout, CassandraCQLExecutionService.resolveTimeoutOverride(new QueryOverrides(null, timeout)));
    }
}
