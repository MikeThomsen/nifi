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
import org.apache.nifi.service.cql.api.service.CQLQueryCallback;

/**
 * One cell of a result row: a column, and the value under it.
 *
 * <p>This is deliberately schema-free. Where {@link CQLQueryCallback} delivers rows as NiFi records - with
 * type inference, a schema, and value conversion - a cell hands back what the server sent. That suits a
 * caller which already owns the encoding of its data, such as one storing opaque serialized bytes, and which
 * would only have to undo any conversion applied on its behalf.
 */
public interface CqlCell {

    /**
     * The column name, exactly as the server named it.
     *
     * <p>Not necessarily a legal CQL identifier: a projected function is named after the expression that
     * produced it, so {@code writetime(v)} is a column name, and a conditional write's outcome column is
     * named {@code [applied]} precisely because the brackets cannot appear in an identifier.
     *
     * <p>Not necessarily unique within a row either - {@code SELECT v, writetime(v), v} returns three cells,
     * two named {@code v}. That is why {@link CqlRow} is a list of cells rather than a map.
     */
    String columnName();

    /**
     * Whether this cell holds a CQL null. Distinct from the column being absent from the row, which is a
     * row-level question answered by {@link CqlRow#findCell(String)}.
     */
    boolean isNull();

    /**
     * The value as binary, or {@code null} if the cell is null.
     *
     * <p>The returned array is a copy the caller may retain and modify freely. This is the accessor for a
     * caller storing opaque bytes, and the only one for which an implementation is required to normalize the
     * representation it received from the backend.
     *
     * @throws UnsupportedOperationException if the column's type is not binary
     */
    byte[] getBytes();

    /**
     * The value as the backend decoded it, with no conversion applied, or {@code null} if the cell is null.
     *
     * <p>Implementations return only types from the JDK. A column whose CQL type has no JDK equivalent - a
     * user defined type, a tuple, or {@code duration} - is not supported here and throws, because the
     * backend's representation of those is a driver class, and letting one cross this boundary is exactly
     * what this API exists to prevent. Use {@link CQLExecutionService#query} for those columns; it has the
     * mapping machinery they need.
     *
     * @throws UnsupportedOperationException if the column's type has no JDK representation
     */
    Object getObject();
}
