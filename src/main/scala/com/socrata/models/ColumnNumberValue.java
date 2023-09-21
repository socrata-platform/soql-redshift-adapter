package com.socrata.models;

import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
        allOf = ColumnValue.class
)
public interface ColumnNumberValue extends ColumnValue{
    Integer getValue();
}
