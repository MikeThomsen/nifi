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

/**
 * Identifies a single column of a single keyspace-qualified table, used as the key type for the
 * {@code primaryKeyOverrides} map {@code CQLExecutionService#insert}/{@code update}/{@code delete} accept -
 * each entry maps one column, on one table, to a {@code RecordPath} that resolves that column's value from
 * a record instead of a same-named record field. Unlike {@link PrimaryKeyMetadata}, this type is built from
 * caller-supplied configuration (for example, a {@code PutCQLRecord} dynamic property name), not from the
 * cluster's own schema metadata, so its {@code fieldName} is not guaranteed to already be in the
 * driver-normalized form {@link PrimaryKeyMetadata#name()} is.
 *
 * @param keyspace the keyspace the target table belongs to
 * @param tableName the target table's name, without the keyspace
 * @param fieldName the target column's name, as supplied by the caller
 */
public record PrimaryKeyIdentifier(String keyspace, String tableName, String fieldName) {
}
