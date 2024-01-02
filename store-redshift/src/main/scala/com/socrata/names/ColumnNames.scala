package com.socrata.store.names

import jakarta.enterprise.context.ApplicationScoped
import com.socrata.datacoordinator.secondary._

object ColumnNames {
  def from(columnInfo: ColumnInfo[_]): String =
    s"${columnInfo.fieldName.get.name.replace(" ", "_").toLowerCase()}_${columnInfo.systemId.underlying}"
}
