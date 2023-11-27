package com.socrata.common.sqlizer.metatypes

import scala.collection.{mutable => scm}

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.OrderedMap

import com.socrata.datacoordinator.id.{UserColumnId, DatasetInternalName}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}

trait CopyCache[MT <: MetaTypes] extends LabelUniverse[MT] {
  def apply(dtn: DatabaseTableName): Option[(CopyInfo, OrderedMap[UserColumnId, ColumnInfo[CT]])]

  def allCopies: Seq[CopyInfo]
}
