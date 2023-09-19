package com.socrata.api;

import com.socrata.models.RollupRequest;
import com.socrata.models.RollupsResponse;
import io.smallrye.mutiny.Multi;
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/rollups")
public interface RollupsApi {
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    Multi<RollupsResponse> getRollups(@BeanParam RollupRequest rollupRequest);
}
