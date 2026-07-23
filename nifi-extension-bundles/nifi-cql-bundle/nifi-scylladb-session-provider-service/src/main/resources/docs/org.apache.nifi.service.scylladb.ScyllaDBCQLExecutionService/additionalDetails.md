<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# ScyllaDBCQLExecutionService

## Description

_ScyllaDBCQLExecutionService_ is the ScyllaDB variant of `CQLExecutionService`, for use in place of
`CassandraCQLExecutionService` by `PutCQLRecord` and `ExecuteCQLQueryRecord` when the target cluster is
ScyllaDB rather than Apache Cassandra. In code, it is a direct subclass of `CassandraCQLExecutionService`
that adds no properties, overrides no methods, and changes no behavior - every property, type conversion
rule, primary key override mechanism, table metadata lookup, and connection-verification step documented for
`CassandraCQLExecutionService` applies here identically, including the version-1 requirement for `timeuuid`
columns.

This works because ScyllaDB's Java Driver (`com.scylladb:java-driver-core`) is a shard-aware fork of the
DataStax Java Driver that retains the same `com.datastax.oss.driver.api.core.config` package and HOCON
configuration format as the driver `CassandraCQLExecutionService` is built on - so the _Driver Configuration
File_ property, and every other connection property, work unchanged when this service targets a ScyllaDB
cluster instead of a Cassandra cluster.

## Where ScyllaDB and Cassandra genuinely differ

None of the differences below are configured differently through this service - they are properties of the
target cluster's own administration, not of anything exposed here:

* ScyllaDB's server-side TLS configuration (`client_encryption_options` in `scylla.yaml`) is PEM-based
  (`certificate`/`keyfile`/`truststore` files), unlike Cassandra's Java-keystore-based equivalent, and has no
  "optional" mode - once enabled, ScyllaDB refuses a plaintext connection outright rather than accepting
  either. This is a server-side administration difference only: the _SSL Context Service_ property on this
  service is configured exactly the same way regardless of which backend it connects to.
