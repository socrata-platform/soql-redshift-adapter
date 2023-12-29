package com.socrata.db.datasets

import com.socrata.db.meta.entity._
import com.socrata.db.Exists

trait TableCreator {
  def create(dataset: Dataset, blobUrl: String): Exists.Exists[String]
}
