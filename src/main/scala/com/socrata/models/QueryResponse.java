package com.socrata.models;

import java.util.Collection;

public interface QueryResponse {
    Collection<ColumnIdValue> getColumns();
}
