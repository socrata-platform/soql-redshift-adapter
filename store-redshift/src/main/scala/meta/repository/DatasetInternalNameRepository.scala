package meta.repository

import io.quarkus.agroal.DataSource
import io.quarkus.hibernate.orm.panache.PanacheRepository
import jakarta.enterprise.context.ApplicationScoped
import meta.entity.DatasetInternalName

@ApplicationScoped
class DatasetInternalNameRepository extends PanacheRepository[DatasetInternalName] {

}
