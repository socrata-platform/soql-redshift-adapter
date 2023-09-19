package com.socrata.models;

import lombok.Data;
import org.jboss.resteasy.reactive.RestQuery;

@Data
public class SchemaRequest {
    @RestQuery
    final String copy;
    @RestQuery
    final Boolean fieldName;
    @RestQuery
    final String ds;
    @RestQuery
    final String rn;
}
