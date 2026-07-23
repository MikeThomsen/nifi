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

import java.util.List;

/**
 * A table's primary key structure, as returned by {@code CQLExecutionService#getMetadata}. Column-name
 * matching against other structures (such as {@link PrimaryKeyIdentifier}) is the caller's responsibility;
 * this type only reports what the cluster's own schema metadata says.
 *
 * @param partitionKey the table's partition key columns, in their real declared order (the order that
 * determines how Cassandra/ScyllaDB compute a row's token)
 * @param clusteringKeys the table's clustering columns, in their real declared order (the order that
 * determines row order within a partition); empty if the table has none
 */
public record PrimaryKey(List<PrimaryKeyMetadata> partitionKey, List<PrimaryKeyMetadata> clusteringKeys) {

}
