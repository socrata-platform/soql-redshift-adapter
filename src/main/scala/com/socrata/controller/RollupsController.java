package com.socrata.controller;

import com.socrata.api.RollupsApi;
import com.socrata.models.RollupRequest;
import com.socrata.models.RollupsResponse;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class RollupsController  implements RollupsApi {
    @Override
    public Multi<RollupsResponse> getRollups(RollupRequest rollupRequest) {
        return Multi.createFrom().empty();
    }
}
