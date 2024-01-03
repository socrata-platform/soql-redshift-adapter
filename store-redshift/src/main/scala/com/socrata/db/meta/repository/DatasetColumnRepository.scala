package com.socrata.db.meta.repository

import com.socrata.db.meta.entity._
import io.quarkus.hibernate.orm.panache._
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class DatasetColumnRepository extends PanacheRepository[DatasetColumn] {}
