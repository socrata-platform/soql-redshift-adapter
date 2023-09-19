package com.socrata.models;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.DiscriminatorMapping;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
        type = SchemaType.OBJECT,
        title = "ColumnValue",
        anyOf = {ColumnTextValue.class, ColumnNumberValue.class},
        discriminatorProperty = "columnType",
        discriminatorMapping = {
                @DiscriminatorMapping(value = "Text", schema = ColumnTextValue.class),
                @DiscriminatorMapping(value = "Number", schema = ColumnNumberValue.class)
        }
)
public interface ColumnValue {
    String getColumnType();
}
