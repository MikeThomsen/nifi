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

package org.apache.nifi.processors.cql.mock;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.service.cql.api.CQLExecutionService;
import org.apache.nifi.service.cql.api.CQLQueryCallback;
import org.apache.nifi.service.cql.api.CqlBatchType;
import org.apache.nifi.service.cql.api.QueryOverrides;
import org.apache.nifi.service.cql.api.UpdateMethod;
import org.apache.nifi.service.cql.api.WriteOverrides;
import org.apache.nifi.service.cql.api.metadata.PrimaryKey;
import org.apache.nifi.service.cql.api.metadata.PrimaryKeyIdentifier;
import org.apache.nifi.service.cql.api.metadata.QualifiedTableName;
import org.apache.nifi.record.path.RecordPath;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This test class exists to support @see org.apache.nifi.processors.cassandra.ExecuteCQLQueryRecordTest
 */
public class MockCQLQueryExecutionService extends AbstractControllerService implements CQLExecutionService {
    private final Iterator<org.apache.nifi.serialization.record.Record> records;
    private QueryOverrides lastQueryOverrides;

    public MockCQLQueryExecutionService(Iterator<org.apache.nifi.serialization.record.Record> records) {
        this.records = records;
    }

    @Override
    public void query(String cql, boolean cacheStatement, List parameters, CQLQueryCallback callback, QueryOverrides overrides) {
        this.lastQueryOverrides = overrides;
        long rowNumber = 1;
        while (records.hasNext()) {
            callback.receive(rowNumber++, records.next(), records.hasNext());
        }
    }

    public QueryOverrides getLastQueryOverrides() {
        return lastQueryOverrides;
    }

    @Override
    public void insert(QualifiedTableName table, org.apache.nifi.serialization.record.Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides, WriteOverrides overrides) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insert(QualifiedTableName table, List<org.apache.nifi.serialization.record.Record> records, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides, CqlBatchType batchType, WriteOverrides overrides) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTransitUrl(QualifiedTableName tableName) {
        return "";
    }

    @Override
    public void delete(QualifiedTableName cassandraTable, org.apache.nifi.serialization.record.Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides, List<String> updateKeys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void update(QualifiedTableName cassandraTable, org.apache.nifi.serialization.record.Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides, List<String> updateKeys, UpdateMethod updateMethod, WriteOverrides overrides) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void update(QualifiedTableName cassandraTable, List<Record> records, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides, List<String> updateKeys, UpdateMethod updateMethod, CqlBatchType batchType, WriteOverrides overrides) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrimaryKey getMetadata(QualifiedTableName table) {
        return null;
    }
}
