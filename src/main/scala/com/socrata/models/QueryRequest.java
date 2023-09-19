package com.socrata.models;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.jboss.resteasy.reactive.RestHeader;
import org.jboss.resteasy.reactive.RestQuery;

@Data
@NoArgsConstructor
public class QueryRequest {

    @RestQuery
    public String context;
    @RestQuery
    public String dataset;
    @RestQuery
    public String query;
    @RestQuery
    public String rowCount;
    @RestQuery
    public String copy;
    @RestQuery
    public String rollupName;
    @RestQuery
    public Boolean obfuscateId;
    @RestQuery
    public Long queryTimeoutSeconds;
    @RestQuery("X-Socrata-Debug")
    public Boolean debug;
    @RestHeader("X-Socrata-Analyze")
    public Boolean analyze;
    @RestHeader("If-Modified-Since")
    public Boolean modifiedSince;
    @RestHeader("X-Socrata-Last-Modified")
    public Boolean modifiedLast;
}
