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

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import org.apache.nifi.service.cql.api.CqlConsistencyLevel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The ScyllaDB half of {@link org.apache.nifi.service.cassandra.CqlConsistencyLevelTest}.
 *
 * <p>This is not a copy for its own sake. ScyllaDB's shard-aware fork ships its <em>own</em>
 * {@code com.datastax.oss.driver.api.core.DefaultConsistencyLevel}, and this module's NAR excludes the
 * DataStax artifact so the fork is the only provider of those packages at runtime. The two enums are separate
 * classes on separate classpaths that happen to share a name, and nothing keeps them in step with each other
 * or with {@link CqlConsistencyLevel} - so a level the fork drops or renames would break only the ScyllaDB
 * service, and only at runtime. Asserting it here is what makes the {@code @Test} in the sibling module
 * insufficient on its own.
 */
class ScyllaConsistencyLevelTest {

    @ParameterizedTest
    @EnumSource(CqlConsistencyLevel.class)
    @DisplayName("Every declared level's value is a name ScyllaDB's driver fork can parse")
    void testValueParsesAsForkConsistencyLevel(final CqlConsistencyLevel level) {
        assertDoesNotThrow(() -> DefaultConsistencyLevel.valueOf(level.getValue()),
                () -> level.name() + " declares the value '" + level.getValue()
                        + "', which ScyllaDB's driver fork does not recognise. Known levels: "
                        + Arrays.stream(DefaultConsistencyLevel.values())
                                .map(Enum::name)
                                .collect(Collectors.joining(", ")));
    }

    @ParameterizedTest
    @EnumSource(CqlConsistencyLevel.class)
    @DisplayName("The declared value matches the fork level it resolves to")
    void testValueMatchesTheResolvedForkLevel(final CqlConsistencyLevel level) {
        assertEquals(level.getValue(), DefaultConsistencyLevel.valueOf(level.getValue()).name());
    }
}
