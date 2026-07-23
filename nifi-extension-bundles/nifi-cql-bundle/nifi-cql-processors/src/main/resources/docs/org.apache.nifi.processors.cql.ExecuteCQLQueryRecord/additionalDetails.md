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

# ExecuteCQLQueryRecord

## Description

_ExecuteCQLQueryRecord_ runs a CQL `SELECT` query against Apache Cassandra or ScyllaDB, through the
connection provided by the configured _Cassandra Connection Provider_ controller service (either
`CassandraCQLExecutionService` or `ScyllaDBCQLExecutionService`), and writes the result set using the
configured _Result Set Output Writer_ (any `RecordSetWriterFactory`, so JSON, Avro, CSV, etc. are all valid
targets). Results stream row-by-row rather than buffering the whole result set in memory, so arbitrarily
large result sets are supported.

The processor can run two ways:

* **Triggered by an incoming FlowFile** - the query, and any property using
  [Expression Language](https://nifi.apache.org/docs/nifi-docs/html/expression-language-guide.html), is
  evaluated against that FlowFile's attributes. The incoming FlowFile is routed to `original` on success,
  `retry` on a query failure, or `failure` on any other processing error.
* **Triggered on a schedule (timer or cron)**, with no incoming connection - useful for a polling query that
  isn't driven by upstream flow content. In this mode there is no FlowFile to evaluate Expression Language
  against, so property values must already be resolvable without one, and a processing error yields the
  processor rather than routing anything (there's no FlowFile to route).

The `executecql.row.count` attribute on the routed FlowFile(s) is not written directly by this processor;
row-count reporting comes from the fragmentation attributes described below, which is the accurate signal
when a single query's results are split across more than one FlowFile.

## Data types

Result set values are converted to an Avro schema (and from there, whatever the configured Record Writer
produces) using the same type mapping `PutCQLRecord` uses for writes, in reverse:

* `BOOLEAN` &rarr; boolean; `TINYINT`/`SMALLINT`/`INT` &rarr; int (Avro has no byte/short primitive, so these
  widen); `BIGINT`/`COUNTER` &rarr; long; `FLOAT` &rarr; float; `DOUBLE` &rarr; double; `BLOB` &rarr; bytes.
* `ASCII`/`TEXT`, `TIMESTAMP`, `UUID`, `TIMEUUID`, `INET`, `VARINT`, `DATE`, `TIME`, and `DECIMAL` are all
  represented as a string field - see [Reading a timeuuid or uuid column](#reading-a-timeuuid-or-uuid-column)
  below for a caveat specific to `UUID`/`TIMEUUID`.
* `LIST`/`SET` become an Avro array, `MAP` becomes an Avro map, and a Cassandra User Defined Type (UDT)
  becomes a nested named Avro record, all recursively - a UDT nested inside another UDT, or inside a
  `LIST`/`SET`/`MAP`, is resolved the same way with no special-casing. Note that Avro maps require string
  keys: a CQL `map` whose key type isn't already string-like is represented with its real (non-string) key
  objects in the underlying data even though the declared schema implies string keys - see the caveat below,
  which applies to this the same way it applies to `UUID`/`TIMEUUID`.
* A CQL type with no mapping defined (for example `DURATION`) fails the query rather than silently dropping
  or mis-typing the column.

### Reading a timeuuid or uuid column

`UUID` and `TIMEUUID` columns are both declared as a string field in the generated schema, but the actual
value placed into the resulting Record is the raw `java.util.UUID` object the driver decoded, not an
already-stringified value - the two are only reconciled if and when something downstream calls
`getAsString(...)` (or another API that coerces through the field's declared type), which converts it via
`UUID.toString()` - a complete, lossless, canonical rendering of all 128 bits, so the *value* itself is never
corrupted or truncated by this. In practice this is transparent for typical usage, since RecordSetWriters
generally coerce a non-`String` object into the declared `String` field rather than requiring an exact type
match. It's worth knowing about mainly because it means the schema's declared type (`string`) and the
record's actual in-memory value type (`UUID`) don't literally agree until something forces the coercion -
not a correctness problem in ordinary use, but relevant if you're writing custom downstream logic against
the record data directly (for example a script or a custom writer) that does a strict type check rather than
going through the Record API's own accessors.

A `timeuuid` column's value is always a genuine version-1 (time-based) UUID by the time it's read back -
Cassandra/ScyllaDB do not accept a non-version-1 value being written to a `timeuuid` column in the first
place (see `PutCQLRecord`'s documentation for what happens on that write-side rejection), so there is nothing
extra to validate on the read side.

## Pagination and query overrides

* _Fetch Size_ overrides how many rows the driver requests per page from the server for this query only,
  without changing the connection service's own configured default.
* _Max Wait Time_ overrides the connection service's configured Read Timeout for this query only.

Both are optional; when unset, the connection service's own configured values apply.

## Splitting results across FlowFiles

* _Max Rows Per Flow File_ (default `0`, meaning "one FlowFile for the whole result set") caps how many rows
  go into a single output FlowFile, splitting a large result set into several. When this produces more than
  one FlowFile, all of them share a `fragment.identifier` attribute, each gets a `fragment.index` giving its
  position among the set, and (unless _Output Batch Size_ is also set - see below) a `fragment.count`
  attribute giving the total, so they can be correlated and reassembled downstream.
* _Output Batch Size_ (default `0`, meaning "commit once at the end") commits the session, releasing output
  FlowFiles downstream, every time this many are ready, instead of waiting for the entire result set to
  finish. This bounds memory/queue growth for very large result sets at the cost of releasing FlowFiles
  before the final row count is known - which is exactly why `fragment.count` is intentionally left unset
  whenever _Output Batch Size_ is configured: earlier FlowFiles may already have been committed downstream
  before the true total could be determined.

If the incoming FlowFile's query returns zero rows, no output FlowFile is created at all, and the incoming
FlowFile (if any) is routed directly to `original`.

## Relationships

* `success` - one or more FlowFiles containing query results.
* `original` - the incoming FlowFile that triggered the query (only present when triggered by an incoming
  FlowFile), routed here once every resulting output FlowFile has been transferred to `success` - or
  immediately, if the query returned no rows at all. Auto-terminated by default, since most flows only need
  the query results themselves, not the triggering FlowFile.
* `failure` - the query could not be executed (for example, invalid CQL, or a schema mismatch between the
  result set and the configured Record Writer).
* `retry` - the query failed in a way that may succeed if attempted again (for example, a transient
  connectivity or consistency-level failure), penalizing the FlowFile before routing it here.
