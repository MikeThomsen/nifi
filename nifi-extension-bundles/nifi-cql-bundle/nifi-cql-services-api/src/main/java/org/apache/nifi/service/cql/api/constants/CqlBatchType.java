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

package org.apache.nifi.service.cql.api.constants;

import org.apache.nifi.service.cql.api.service.CQLExecutionService;

/**
 * The type of batch statement to use when {@link CQLExecutionService} writes more than one record at a time.
 * Mirrors Cassandra's own batch semantics: {@code COUNTER} is required for counter mutations, which cannot be
 * mixed into a {@code LOGGED} or {@code UNLOGGED} batch.
 */
public enum CqlBatchType {
    LOGGED,
    UNLOGGED,
    COUNTER
}
