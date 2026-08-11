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
package org.apache.nifi.service.cql.api.service;

import java.time.Duration;

/**
 * Per-call overrides for a single {@link CQLExecutionService#insert} or {@link CQLExecutionService#update}
 * invocation. Any field left {@code null} means "use the value configured on the {@link CQLExecutionService}
 * itself". Additional write overrides can be added here as new record components without changing the
 * {@code insert}/{@code update} method signatures again.
 *
 * @param ttl the Time To Live to apply, or {@code null} to use the connection service's configured default (if any)
 * @param timestampField the name of a field in each record whose value supplies the CQL write timestamp for that
 * record, instead of the current time, or {@code null} to use the current time as usual. Resolved per record - unlike
 * {@code ttl}, this is not a single value applied to the whole call, since different records can carry different
 * event times. Ignored for Increment/Decrement updates, since Cassandra/ScyllaDB do not support a custom write
 * timestamp on counter columns.
 */
public record WriteOverrides(Duration ttl, String timestampField) {
    public static final WriteOverrides NONE = new WriteOverrides(null, null);
}
