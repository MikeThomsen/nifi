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
package org.apache.nifi.service.cql.api.lookup;

import org.apache.nifi.service.cql.api.service.CQLExecutionService;

import java.util.List;

/**
 * The outcome of a statement executed through {@link CQLExecutionService#execute}.
 */
public interface CqlStatementResult {

    /**
     * Whether a conditional statement - one carrying {@code IF NOT EXISTS}, {@code IF EXISTS}, or an
     * {@code IF <condition>} clause - was applied.
     *
     * <p>Always {@code true} for an unconditional statement, so a caller that never writes conditionally has
     * no special case to handle.
     *
     * <p>This is a property of the statement rather than of a row, which is why it is here and not a column.
     * The backends do report it as one - a boolean column named {@code [applied]} - but that name is not a
     * legal identifier anywhere, and modelling it as data rather than as an outcome pushes that problem onto
     * every caller. {@link #rows()} therefore does not contain it.
     */
    boolean wasApplied();

    /**
     * The rows the server returned. Never {@code null}; frequently empty.
     *
     * <p>For a conditional write that was <em>rejected</em>, this is the row that is actually stored - which
     * is what allows a compare-and-set to report the current value without a second round trip. For a
     * conditional write that applied, and for an unconditional write, this is empty. For a {@code SELECT},
     * these are the selected rows.
     *
     * <p>The empty-when-applied rule is a normalization implementations must perform, not something the
     * backends agree on: given an applied {@code IF NOT EXISTS}, Cassandra returns the outcome column alone
     * while ScyllaDB also returns the row's other columns. Passing that difference through would make
     * {@code rows().isEmpty()} mean different things on different backends, which is precisely what a caller
     * uses this API to avoid.
     */
    List<CqlRow> rows();
}
