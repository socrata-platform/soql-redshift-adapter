package com.socrata.names

import com.socrata.db.meta.entity._

import jakarta.enterprise.context.ApplicationScoped
import com.socrata.datacoordinator.secondary._

object TableName {
  def from(dataset: DatasetInfo): String = s"${dataset.internalName}"
}
