package com.socrata.store.column

import jakarta.enterprise.context.ApplicationScoped
import com.socrata.datacoordinator.secondary._

trait ColumnNames {
  def name(columnInfo: ColumnInfo[_]): String
}

@ApplicationScoped
object ColumnNamesImpl extends ColumnNames {
  override def name(columnInfo: ColumnInfo[_]): String =
    s"${columnInfo.fieldName.get.name.replace(" ", "_").toLowerCase()}_${columnInfo.systemId.underlying}"
}
