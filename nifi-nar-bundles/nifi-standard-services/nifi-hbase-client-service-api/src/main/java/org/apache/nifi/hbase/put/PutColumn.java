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
package org.apache.nifi.hbase.put;

import org.apache.nifi.util.StringUtils;

/**
 * Encapsulates the information for one column of a put operation.
 */
public class PutColumn {

    private final byte[] columnFamily;
    private final byte[] columnQualifier;
    private final byte[] buffer;
    private final String visibility;
    private final Long timestamp;


    public PutColumn(final byte[] columnFamily, final byte[] columnQualifier, final byte[] buffer) {
        this(columnFamily, columnQualifier, buffer, null, null);
    }

    public PutColumn(final byte[] columnFamily, final byte[] columnQualifier, final byte[] buffer, final String visibility) {
        this(columnFamily, columnQualifier, buffer, null, visibility);
    }

    public PutColumn(final byte[] columnFamily, final byte[] columnQualifier, final byte[] buffer, final Long timestamp) {
        this(columnFamily, columnQualifier, buffer, timestamp, null);
    }

    public PutColumn(final byte[] columnFamily, final byte[] columnQualifier, final byte[] buffer, final Long timestamp, final String visibility) {
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
        this.buffer = buffer;
        this.timestamp = timestamp;
        this.visibility = !StringUtils.isEmpty(visibility) ? visibility : null;
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    public byte[] getColumnQualifier() {
        return columnQualifier;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public String getVisibility() {
        return visibility;
    }

    public Long getTimestamp() {
        return timestamp;
    }

}
