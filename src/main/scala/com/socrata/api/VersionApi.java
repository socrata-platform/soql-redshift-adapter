package com.socrata.api;


import com.socrata.models.VersionResponse;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/version")
public interface VersionApi {

    @GET
    @Produces({MediaType.APPLICATION_JSON})
    Uni<VersionResponse> getVersion();
}
