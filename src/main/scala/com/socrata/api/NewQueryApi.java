package com.socrata.api;

import com.socrata.models.NewQueryRequest;
import com.socrata.models.NewQueryResponse;
import io.smallrye.mutiny.Multi;
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/new-query")
public interface NewQueryApi {
    @POST
    @Produces({MediaType.APPLICATION_JSON})
    Multi<NewQueryResponse> postNewQuery(@BeanParam NewQueryRequest newQueryRequest);
}
