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

# CassandraCQLExecutionService

## Description

_CassandraCQLExecutionService_ implements `CQLExecutionService`, the connection abstraction that
`PutCQLRecord`, `ExecuteCQLQueryRecord` and `CQLDistributedMapCache` depend on to talk to Apache Cassandra.
It owns a single shared `CqlSession` for the lifetime of the controller service: the session is opened once,
when the service is enabled, and closed when the service is disabled.

Every component above reaches the cluster through this service rather than through a driver of its own, so
the connection is configured **once, here** - contact points, credentials, TLS, and the driver configuration
below are shared by all of them, and none of those settings are repeated on the components themselves.
`CQLDistributedMapCache` in particular opens no session at all; its module carries no database driver on the
classpath at any scope, and it takes this service through its _CQL Execution Service_ property.

For a ScyllaDB cluster, use `ScyllaDBCQLExecutionService` instead. It is a subclass of this service and
behaves identically - everything documented here applies to it unchanged.

## Properties

* **Cassandra Contact Points** - comma-separated `host:port` addresses of cluster nodes. Default port is
  9042 if a port isn't specified per contact point, but at least one contact point is required.
* **Cassandra Datacenter** - the local datacenter to use with the driver's load balancing.
* **Default Keyspace** - the keyspace the session connects to by default. If not set, an unqualified table
  name passed to this service will fail to resolve against Cassandra's own schema metadata (used by
  `getMetadata`); table/keyspace-qualified query-builder statements (`INSERT`/`UPDATE`/`DELETE`) fall back to
  whatever keyspace the driver session itself was opened against.
* **Username** / **Password** - credentials for the cluster, only applied if both are set to a non-blank
  value.
* **SSL Context Service** - an `SSLContextService` providing client certificate/trust material for a TLS
  connection. Optional; if unset, the connection is unencrypted. Mutual TLS - a cluster configured with
  `require_client_auth: true` - needs no separate setting here: it is enabled purely by referencing a service
  that has a keystore configured, whose certificate is what this service presents when the cluster asks for
  one.
* **Fetch size** - default page size for query result sets - how many rows the driver requests per round
  trip, not a cap on total rows returned; `0` means the driver's own default page size (5000) is used. Can be
  overridden per query via `ExecuteCQLQueryRecord`'s own _Fetch Size_ property.
* **Read Timeout** / **Connect Timeout** - driver-level request and connection timeouts. _Read Timeout_ can be
  overridden per query via `ExecuteCQLQueryRecord`'s own _Max Wait Time_ property.
* **Consistency Level** - the driver's default request consistency level.
* **Compression Type** - transport-level compression for requests/responses.
* **Default Time To Live** - the default TTL applied to `INSERT` and `SET`-method `UPDATE` statements when
  the calling processor doesn't supply its own override. Never applied to counter mutations, since
  Cassandra/ScyllaDB do not support a TTL on counter columns.
* **Driver Configuration File** - path to an optional Java Driver `application.conf`-style (HOCON) file for
  settings not otherwise exposed as a property here (load balancing policy, retry policy, speculative
  execution, etc.). Settings in the file take precedence; anything the file doesn't set falls back to the
  properties above or the driver's own built-in defaults.

## Type conversion (write path)

Beyond the driver's own default codecs, this service registers additional codecs so a compatible-but-non-exact
Java value (for example, a numeric `String` for an integer column) still binds correctly to `boolean`,
`tinyint`, `smallint`, `int`, `bigint`/`counter`, `float`, `double`, and `char`-mapped columns, plus
`java.sql.Date`/`java.sql.Time`/`java.sql.Timestamp` handling for the corresponding CQL date/time types.
Beyond codec registration, this service also:

* Accepts a nested `Record` or a raw `Map` for a User Defined Type column, recursing into nested UDTs, and
  into UDTs held inside a `list`/`set`/`map` column, with no special-casing required at the call site.
* Accepts a NiFi `ARRAY` (or a raw `Object[]`, NiFi's own canonical array representation) for `list`/`set`
  columns.
* Normalizes a compatible value (including a `String` representation) into a real `java.util.UUID` for
  `uuid` columns, and for `timeuuid` columns as well - with one additional, hard requirement:

### timeuuid columns must be a genuine version-1 UUID

A `timeuuid` column's value must be an actual version 1 (time-based) UUID - this is part of the CQL type's
own definition, not something specific to this service. If the normalized value's version is not 1, the
write fails immediately with a message naming the offending value and its actual version, for example:

```
Value '87ccc9c5-c4fc-4073-98bc-ee6e7e538237' is not a valid timeuuid: version 4, but timeuuid columns require a version 1 (time-based) UUID
```

Without this check, the same bad value would still fail - the underlying driver rejects it too - but with a
far less clear error (`CodecNotFoundException: Codec not found for requested operation: [TIMEUUID <->
java.util.UUID]`) that reads like a driver/codec configuration problem rather than a data problem. Supplying
a real version-1 value (for example, one generated by the DataStax Java Driver's `Uuids.timeBased()`) avoids
this entirely; a plain `UUID.randomUUID()` value (version 4) does not.

## Primary key overrides

`insert`/`update`/`delete` accept a map of primary key column overrides, keyed by keyspace/table/column, each
mapped to a compiled `RecordPath`. When a bind-marker column has a matching override, its value is resolved
by evaluating that `RecordPath` against the current record instead of looking up a same-named record field.
The `RecordPath` must resolve to exactly one value; resolving to zero or more than one values fails that
record's write. Column name matching between an override and the statement's bind markers is done via
`CqlIdentifier`-normalized comparison, so a dynamic property targeting a mixed-case column name still matches
a lowercased, unquoted column as Cassandra itself would interpret it. This is the mechanism
`PutCQLRecord`'s primary key dynamic properties are built on; see that processor's documentation for the
property name/value format.

## Table metadata

`getMetadata` returns a table's primary key structure (partition key columns and clustering columns, each
with their declared order) by querying the driver's own schema metadata and caching the result per
keyspace-qualified table for the life of the enabled service - safe to cache indefinitely within a session,
since Cassandra does not allow altering a table's partition/clustering key columns after creation.

## Connection verification

Enabling this service (or clicking "Verify" in the NiFi UI) runs up to three checks:

1. **Establish Connection** - opens a session with the configured contact points/credentials/TLS settings.
2. **Verify Datacenter** - executes a trivial query (`SELECT release_version FROM system.local`) to force the
   driver to select a node, since `CqlSession` construction alone can succeed even when no node belongs to
   the configured datacenter (the driver only filters by datacenter when computing a query plan for an
   actual request) - this step catches a misconfigured datacenter at verification time instead of on the
   first real query.
3. **Verify Keyspace** - only run if _Default Keyspace_ is set; confirms the keyspace actually exists in the
   cluster's schema metadata.
