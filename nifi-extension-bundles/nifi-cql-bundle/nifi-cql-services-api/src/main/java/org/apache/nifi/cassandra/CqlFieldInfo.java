package org.apache.nifi.cassandra;

public class CqlFieldInfo {
    private String fieldName;
    private String dataType;
    private int dataTypeProtocolCode;

    public CqlFieldInfo(String fieldName, String dataType, int dataTypeProtocolCode) {
        this.fieldName = fieldName;
        this.dataType = dataType;
        this.dataTypeProtocolCode = dataTypeProtocolCode;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getDataType() {
        return dataType;
    }

    public int getDataTypeProtocolCode() {
        return dataTypeProtocolCode;
    }
}