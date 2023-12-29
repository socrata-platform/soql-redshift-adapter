package com.socrata.store.blob

import com.socrata.db.meta.entity._

trait BlobNames {
  def name(dataset: Dataset): String
}

object BlobNames extends BlobNames {
  override def name(dataset: Dataset): String = ???
}
