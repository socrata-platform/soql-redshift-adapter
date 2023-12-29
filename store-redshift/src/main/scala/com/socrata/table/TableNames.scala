package com.socrata.store.table

import com.socrata.db.meta.entity._

import jakarta.enterprise.context.ApplicationScoped
import com.socrata.datacoordinator.secondary._

trait TableName {
  def name(dataset: Dataset): String
}

object TableName extends TableName {
  override def name(dataset: Dataset): String = ??? // using the dataset.tableName, create tableName
}
