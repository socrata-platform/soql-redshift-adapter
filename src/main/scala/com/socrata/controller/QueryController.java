package com.socrata.controller;

import com.socrata.api.QueryApi;
import com.socrata.models.*;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.Collection;
import java.util.Collections;

@ApplicationScoped
public class QueryController implements QueryApi {
    @Override
    public Multi<QueryResponse> getQuery(QueryRequest queryRequest) {
        return Multi.createFrom().item(new QueryResponse() {
            @Override
            public Collection<ColumnIdValue> getColumns() {
                return Collections.singletonList(new ColumnIdValue() {
                    @Override
                    public ColumnId getColumnId() {
                        return new ColumnId() {
                            @Override
                            public Long getId() {
                                return 1L;
                            }
                        };
                    }

                    @Override
                    public ColumnValue getColumnValue() {
                        return new ColumnTextValue() {
                            @Override
                            public String getValue() {
                                return "test";
                            }
                        };
                    }
                });
            }
        });
    }

    @Override
    public Multi<QueryResponse> postQuery(QueryRequest queryRequest) {
        return Multi.createFrom().empty();
    }
}
