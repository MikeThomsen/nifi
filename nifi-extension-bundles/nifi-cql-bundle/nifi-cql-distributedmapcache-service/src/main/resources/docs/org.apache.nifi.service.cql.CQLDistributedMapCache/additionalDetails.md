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

# CQLDistributedMapCache

## Description

_CQLDistributedMapCache_ implements NiFi's `DistributedMapCacheClient` interface backed by Apache Cassandra
or ScyllaDB. It opens and manages its own `CqlSession` independently of `CassandraCQLExecutionService`/
`ScyllaDBCQLExecutionService` - it does not depend on either of those controller services, though it exposes
the same connection property names for a consistent configuration experience.

By default (_Partitioning Strategy_ = Simple Key/Value), each cache entry is stored as a single row keyed by
its own partition key in an operator-managed table - one partition per key, no clustering columns. An opt-in
Bucketed strategy is also available for large caches on small/cost-constrained clusters; see
[Partitioning Strategy](#partitioning-strategy) below.

**The backing table is not created by this service and must already exist.** Its _Key Column Name_ and
_Value Column Name_ columns must both be of CQL type `blob`: the `Serializer`/`Deserializer` NiFi passes into
each call erase the caller's original Java type into opaque bytes before they ever reach this service, so
`blob` is the only column type that can safely hold them without this service attempting to reinterpret an
encoding it has no contract to understand.

## Properties

Connection properties are shared with `CassandraCQLExecutionService`/`ScyllaDBCQLExecutionService`
(_Cassandra Contact Points_, _Cassandra Datacenter_, _Default Keyspace_, _SSL Context Service_, _Username_,
_Password_, _Consistency Level_, _Compression Type_, _Read Timout_, _Connect Timeout_, _Driver Configuration
File_, _Default Time To Live_ - applied as the TTL on `put`/`putIfAbsent` writes when set), plus the
following, specific to this service:

* **Partitioning Strategy** - see [Partitioning Strategy](#partitioning-strategy) below.
* **Partition Count** - the fixed number of hash buckets to spread cache entries across when _Partitioning
  Strategy_ is Bucketed (ignored otherwise). Allowed range is 32,768 to 1,000,000,000. Choose based on
  expected total key count and average value size so no bucket's total size grows beyond a safe partition
  size; when in doubt, prefer a larger value, since undersizing can only be fixed by migrating the entire
  dataset to a new _Partition Count_.
* **Table Name** - the existing table backing the cache.
* **Key Column Name** (default `cache_key`) - the `blob` column storing the cache key. With Simple Key/Value,
  this must be the table's sole partition key. With Bucketed, this must be a clustering column following the
  `bucket` partition key column.
* **Value Column Name** (default `cache_value`) - the `blob` column storing the cache value.
* **Strict Removal** (default `false`) - controls how `remove()` determines its returned boolean; see
  [Method behavior](#method-behavior) below.
* **Maximum Value Size** (default `1 MB`) - the maximum permitted size of a serialized cache value. A larger
  value is rejected before anything is written, guarding against accidentally writing an oversized
  single-partition row.

## Partitioning Strategy

* **Simple Key/Value** (default) - each cache key is its own partition, with no clustering columns. This is
  the idiomatic, safest schema for this access pattern and should remain the default for almost all
  deployments.
* **Bucketed** - cache keys are hashed (via CRC32 of the key's serialized bytes, modulo _Partition Count_)
  into a fixed number of shared partitions, using the key itself as a clustering column within its assigned
  partition. This is only intended for a narrow situation: a very large number of cache entries (hundreds of
  millions or more) on a small or cost-constrained cluster, where per-partition memory overhead (bloom
  filters, indexes) - not raw data volume - is the binding constraint. It trades that memory overhead for a
  real cost: conditional operations (`putIfAbsent`, `getAndPutIfAbsent`, and any compare-and-swap) on
  different keys that happen to hash into the same partition will contend with each other, since
  Cassandra/ScyllaDB serialize lightweight transactions per partition, not per row. _Partition Count_ also
  cannot be safely changed once data has been written without migrating the entire dataset. When Bucketed is
  selected, the backing table needs an additional `bucket` `int` partition-key column ahead of the key
  column.

## Method behavior

* **put** - serializes the value, rejects it before writing if it exceeds _Maximum Value Size_, then executes
  a prepared `INSERT` (with the configured TTL, if any).
* **putIfAbsent** - the same `INSERT`, but conditional (`IF NOT EXISTS`); returns whether the insert was
  actually applied.
* **getAndPutIfAbsent** - attempts `putIfAbsent`. If it succeeds, returns `null` (nothing was previously
  present). If it fails, Cassandra returns the existing row's columns on that same failed lightweight
  transaction's result set, so the current value is read from the same round trip - no second query is
  needed.
* **containsKey** - an existence check (`SELECT ... LIMIT 1`).
* **get** - fetches the value column for the key; `null` if no row exists.
* **remove** - behavior depends on _Strict Removal_: if `true`, a conditional `DELETE ... IF EXISTS` makes
  the returned boolean authoritative under concurrent access, at the cost of a lightweight transaction. If
  `false` (default), a plain existence check followed by an unconditional delete is cheaper, but leaves a
  small race window between the two statements - acceptable for callers that don't treat the returned
  boolean as load-bearing.
* **subMap** - issues all key lookups concurrently (asynchronously), then awaits every result, rather than
  making one round trip per key sequentially.
* **close** - a no-op for this service: the shared `CqlSession` is a controller-service-lifetime resource,
  torn down in `@OnDisabled`, not per-client-close.

## Connection verification

Verifying this service's configuration opens an independent session and runs a single "Establish Connection"
check (`SELECT release_version FROM system.local`) - unlike `CassandraCQLExecutionService`, there is no
separate datacenter or keyspace verification step.
