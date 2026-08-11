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

import java.util.List;
import java.util.Optional;

/**
 * One row of a statement result, exposed without imposing a schema on it.
 *
 * <p>A row is its cells, in the order the columns were selected; every other accessor here is derived from
 * that, so an implementation provides {@link #cells()} and inherits the rest. The list is the primitive
 * rather than a name-keyed map for three reasons: selection order carries information a caller reading an
 * unfamiliar row has no other way to recover, reading several columns by name costs a traversal each, and
 * column names are not unique - {@code SELECT v, writetime(v), v} returns two cells named {@code v}, one of
 * which a map would silently discard.
 */
public interface CqlRow {

    /**
     * Every cell in the row, in the order the columns were selected. Never {@code null}; empty only if the
     * statement selected no columns.
     */
    List<CqlCell> cells();

    /**
     * The column names, in selection order. May contain duplicates, and may contain names that are not legal
     * CQL identifiers - see {@link CqlCell#columnName()}.
     */
    default List<String> columnNames() {
        return cells().stream().map(CqlCell::columnName).toList();
    }

    /**
     * The first cell with this column name, or empty if the row has no such column. Where a name appears more
     * than once, reach for {@link #cells()} instead - this cannot distinguish them.
     */
    default Optional<CqlCell> findCell(final String columnName) {
        return cells().stream().filter(cell -> cell.columnName().equals(columnName)).findFirst();
    }

    /**
     * Shorthand for the single-column lookup most callers want.
     *
     * <p>Note the two different absences: this throws when the row has no such column, which is a coding
     * error, and returns {@code null} when the column exists and holds a CQL null, which is data.
     *
     * @throws IllegalArgumentException if the row has no column of that name
     */
    default byte[] getBytes(final String columnName) {
        return requireCell(columnName).getBytes();
    }

    /**
     * As {@link #getBytes(String)}, for a column whose value is wanted as the backend decoded it.
     *
     * @throws IllegalArgumentException if the row has no column of that name
     */
    default Object getObject(final String columnName) {
        return requireCell(columnName).getObject();
    }

    private CqlCell requireCell(final String columnName) {
        return findCell(columnName).orElseThrow(() -> new IllegalArgumentException(
                "No column named '" + columnName + "'; row has " + columnNames()));
    }
}
