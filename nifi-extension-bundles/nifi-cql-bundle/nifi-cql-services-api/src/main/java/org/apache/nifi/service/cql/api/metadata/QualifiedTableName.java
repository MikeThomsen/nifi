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

package org.apache.nifi.service.cql.api.metadata;

import org.apache.nifi.util.StringUtils;

/**
 * A table name, optionally qualified with its keyspace, passed to every table-taking
 * {@code CQLExecutionService} method in place of a single ambiguous {@code String}. An unqualified instance
 * (empty or {@code null} {@code keyspace}) resolves against whichever keyspace the connection itself
 * defaults to - either the driver session's own default keyspace for statement building, or the connection
 * service's configured default keyspace for schema metadata lookups, which have no equivalent implicit
 * fallback of their own.
 *
 * @param keyspace the keyspace the table belongs to, or {@code null}/empty if unqualified
 * @param table the table's name, without the keyspace
 */
public record QualifiedTableName(String keyspace, String table) {
    /** @return {@code true} if {@code keyspace} is set, i.e. this instance does not need a default keyspace resolved for it */
    public boolean isQualified() {
        return StringUtils.isNotEmpty(keyspace);
    }
}
