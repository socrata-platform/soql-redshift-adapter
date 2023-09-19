package com.socrata.api;

import com.socrata.models.QueryRequest;
import com.socrata.models.QueryResponse;
import io.smallrye.mutiny.Multi;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

@Path("/query")
public interface QueryApi {

    @GET
    @Produces({MediaType.APPLICATION_JSON})
    Multi<QueryResponse> getQuery(@BeanParam QueryRequest queryRequest);

    @POST
    @Produces({MediaType.APPLICATION_JSON})
    Multi<QueryResponse> postQuery(@BeanParam QueryRequest queryRequest);
}
