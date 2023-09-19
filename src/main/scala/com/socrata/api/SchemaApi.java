package com.socrata.api;

import com.socrata.models.SchemaRequest;
import com.socrata.models.SchemaResponse;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/schema")
public interface SchemaApi {

    @GET
    @Produces({MediaType.APPLICATION_JSON})
    Uni<SchemaResponse> getSchema(@BeanParam SchemaRequest schemaRequest);
}
