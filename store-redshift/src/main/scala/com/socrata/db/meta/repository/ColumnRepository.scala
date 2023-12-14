package com.socrata.db.meta.repository

import com.socrata.db.meta.entity.ColumnInfo
import io.quarkus.hibernate.orm.panache.PanacheRepository
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class ColumnRepository extends PanacheRepository[ColumnInfo] {

}
