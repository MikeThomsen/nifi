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

import org.apache.nifi.serialization.record.Record;

/**
 * Receives {@link CQLExecutionService#query} results one row at a time, so a caller can begin acting on rows
 * (for example, writing them to a FlowFile) before the whole result set has been fetched.
 */
public interface CQLQueryCallback {
    /**
     * @param rowNumber the 1-based position of {@code result} within this query's result set
     * @param result a single row, converted to a {@link Record} using a schema derived from the result set's
     * own column metadata
     * @param hasMore {@code true} if at least one more row follows this one; {@code false} on the last row,
     * which is the implementation's signal to finalize whatever it has been accumulating
     */
    void receive(long rowNumber, Record result, boolean hasMore);

    /**
     * This method will be called when a session provider service needs to be able to signal that a fatal error
     * happened and any batched data needs to be cleaned up so the session can be committed properly
     */
    void clear();

    boolean hasSentOriginal();
}
