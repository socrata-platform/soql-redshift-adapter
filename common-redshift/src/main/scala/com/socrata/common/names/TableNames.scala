package com.socrata.common.names


import com.socrata.datacoordinator.secondary._

object TableName {
  def from(dataset: DatasetInfo): String = s"${dataset.internalName}"
}
