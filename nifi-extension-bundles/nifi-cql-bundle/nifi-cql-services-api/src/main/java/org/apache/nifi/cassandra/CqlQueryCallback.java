package org.apache.nifi.cassandra;

import java.util.List;
import java.util.Map;

public interface CqlQueryCallback {
    boolean receive(long rowNumber, List<CqlFieldInfo> columnInformation, Map<String, Object> result);
}
