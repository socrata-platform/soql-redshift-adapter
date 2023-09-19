package com.socrata.controller;

import com.socrata.api.SchemaApi;
import com.socrata.models.SchemaRequest;
import com.socrata.models.SchemaResponse;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class SchemaController implements SchemaApi {
    @Override
    public Uni<SchemaResponse> getSchema(SchemaRequest schemaRequest) {
        return Uni.createFrom().nullItem();
    }
}
