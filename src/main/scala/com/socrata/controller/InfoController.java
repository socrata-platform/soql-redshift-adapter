package com.socrata.controller;

import com.socrata.api.InfoApi;
import com.socrata.models.InfoRequest;
import com.socrata.models.InfoResponse;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class InfoController implements InfoApi {
    @Override
    public Multi<InfoResponse> postInfo(InfoRequest infoRequest) {
        return Multi.createFrom().empty();
    }
}
