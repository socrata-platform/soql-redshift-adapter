package com.socrata.models;

public interface ColumnTextValue extends ColumnValue{
    @Override
    default String getColumnType(){
        return "Text";
    }

    String getValue();
}
