package com.socrata.store.handlers

import com.socrata.datacoordinator.id._
import com.socrata.common.sqlizer._
import com.socrata.store.names._
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

trait Resync {
  def store(
      dataset: Dataset,
      columns: List[DatasetColumn],
      schema: ColumnIdMap[ColumnInfo[SoQLType]],
      data: Iterator[ColumnIdMap[SoQLValue]]): Option[Long]
}

@ApplicationScoped
case class ResyncImpl(jsonTransformer: JsonTransformer, tableCreator: TableCreator) extends Resync {
  override def store(
      dataset: Dataset,
      columns: List[DatasetColumn],
      schema: ColumnIdMap[ColumnInfo[SoQLType]],
      data: Iterator[ColumnIdMap[SoQLValue]]): Option[Long] = {

    val jsons = jsonTransformer
      .transformAll(data, schema)
      .map(JsonUtil.renderJson(_, pretty = false))

    tableCreator.create(SoQLSqlizer.repProvider(null, null))(
      dataset,
      columns.map(column => column -> schema(new ColumnId(column.columnId))),
      ""
    )
    ???
  }
}
