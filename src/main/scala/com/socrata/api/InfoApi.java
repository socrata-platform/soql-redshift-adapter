package com.socrata.api;

import com.socrata.models.InfoRequest;
import com.socrata.models.InfoResponse;
import com.socrata.models.QueryRequest;
import com.socrata.models.QueryResponse;
import io.smallrye.mutiny.Multi;
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/info")
public interface InfoApi {
    @POST
    @Produces({MediaType.APPLICATION_JSON})
    Multi<InfoResponse> postInfo(@BeanParam InfoRequest infoRequest);
}
