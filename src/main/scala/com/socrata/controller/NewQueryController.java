package com.socrata.controller;

import com.socrata.api.NewQueryApi;
import com.socrata.models.NewQueryRequest;
import com.socrata.models.NewQueryResponse;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class NewQueryController implements NewQueryApi {
    @Override
    public Multi<NewQueryResponse> postNewQuery(NewQueryRequest newQueryRequest) {
        return Multi.createFrom().empty();
    }
}
