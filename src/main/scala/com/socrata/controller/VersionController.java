package com.socrata.controller;

import com.socrata.api.VersionApi;
import com.socrata.models.VersionResponse;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class VersionController implements VersionApi {
    @Override
    public Uni<VersionResponse> getVersion() {
       return Uni.createFrom().nullItem();
    }
}
