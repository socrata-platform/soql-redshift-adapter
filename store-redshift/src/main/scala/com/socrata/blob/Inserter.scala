package com.socrata.store.blob
// not really blob actually
// rename resync handler

import com.socrata.store.table._
import com.socrata.store.json._
import com.socrata.db.datasets._
import com.socrata.db.meta.entity._
import com.rojoma.json.v3.ast._
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._
import scala.collection.mutable.ArrayBuilder
import java.io._
import scala.collection.JavaConverters._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types._
import jakarta.enterprise.context.ApplicationScoped
import com.socrata.datacoordinator.secondary._
import com.rojoma.json.v3.util.JsonUtil

trait Inserter {
  def store(
      dataset: Dataset,
      schema: ColumnIdMap[ColumnInfo[SoQLType]],
      data: Iterator[ColumnIdMap[SoQLValue]]): Option[Long]
}

@ApplicationScoped
case class InserterImpl(jsonTransformer: JsonTransformer, dataStorage: DataStorage, tableCreator: TableCreator)
    extends Inserter {
  override def store(
      dataset: Dataset,
      schema: ColumnIdMap[ColumnInfo[SoQLType]],
      data: Iterator[ColumnIdMap[SoQLValue]]): Option[Long] = {

    val jsons = jsonTransformer
      .transformAll(data, schema)
      .map(JsonUtil.renderJson(_))
      .map(_.getBytes)

    dataStorage.store(TableName.name(dataset), jsons)
    tableCreator.create(dataset, BlobNames.name(dataset))
    ???
  }
}
