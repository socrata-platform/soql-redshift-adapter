package com.socrata.store.names

import com.socrata.db.meta.entity._

import jakarta.enterprise.context.ApplicationScoped
import com.socrata.datacoordinator.secondary._

object TableName {
  def from(dataset: Dataset): String = ??? // using the dataset.tableName, create tableName
}
