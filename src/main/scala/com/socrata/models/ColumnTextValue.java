package com.socrata.models;

import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.DiscriminatorMapping;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
        allOf = ColumnValue.class
)
public interface ColumnTextValue extends ColumnValue{
    String getValue();
}
