package com.socrata.models;

public interface ColumnNumberValue extends ColumnValue{
    @Override
    default String getColumnType(){
        return "Number";
    }

    Integer getValue();
}
