package com.socrata.models;

import lombok.Data;
import org.jboss.resteasy.reactive.RestQuery;

@Data
public class RollupRequest {
    @RestQuery
    final Boolean includeUnmaterialized;
    @RestQuery
    final String copy;
    @RestQuery
    final String ds;
    @RestQuery
    final String rn;

}
