package db.meta.repository

import db.meta.entity.Dataset
import io.quarkus.hibernate.orm.panache.PanacheRepository
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class DatasetRepository extends PanacheRepository[Dataset] {

}
