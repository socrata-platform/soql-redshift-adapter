package com.socrata.store.json

import jakarta.enterprise.context.ApplicationScoped
import com.socrata.soql.types._
import com.rojoma.json.v3.ast._
import com.vividsolutions.jts.geom.Geometry
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.secondary._
import com.socrata.store.column._

trait JsonTransformer {
  def transformAll(
      colIdMaps: Iterator[ColumnIdMap[SoQLValue]],
      schema: ColumnIdMap[ColumnInfo[SoQLType]]): Iterator[JValue] = colIdMaps.map(transform(_, schema))

  def transform(colIdMap: ColumnIdMap[SoQLValue], schema: ColumnIdMap[ColumnInfo[SoQLType]]): JValue
}

@ApplicationScoped
case class JsonTransformerImpl(nameMapper: ColumnNames) extends JsonTransformer {
  def transform(colIdMap: ColumnIdMap[SoQLValue], schema: ColumnIdMap[ColumnInfo[SoQLType]]): JValue = {
    val row: Seq[(ColumnInfo[SoQLType], SoQLValue)] = colIdMap.toSeq.map { case (id, value) =>
      schema.get(id).get -> value
    }
    val dbRow: Map[String, SoQLValue] = row.map { case (colInfo, soqlValue) =>
      nameMapper.name(colInfo) -> soqlValue
    }.toMap
    JObject(dbRow.mapValues(transformValue))
  }

  private def transformValue(soqlValue: SoQLValue): JValue = soqlValue match {
    case SoQLText(value) => JString(value)
    case SoQLNumber(value) => JNumber(value)
    case SoQLBoolean(value) => JBoolean(value)
    //    case SoQLFixedTimestamp(value) => JString(value)
    //    case SoQLFloatingTimestamp(value) => JString(value)
    //    case SoQLDate(value) => JString(value)
    //    case SoQLTime(value) => JString(value)
    case SoQLJson(value) => value
    case t @ SoQLPoint(value) =>
      val bytes = t.typ.WkbRep(value)
      val string = bytes.iterator.map { b => "%02x".format(b & 0xff) }.mkString
      JString(string)
    case t @ SoQLMultiPoint(value) =>
      val bytes = t.typ.WkbRep(value)
      val string = bytes.iterator.map { b => "%02x".format(b & 0xff) }.mkString
      JString(string)
    case t @ SoQLLine(value) =>
      val bytes = t.typ.WkbRep(value)
      val string = bytes.iterator.map { b => "%02x".format(b & 0xff) }.mkString
      JString(string)
    case t @ SoQLMultiLine(value) =>
      val bytes = t.typ.WkbRep(value)
      val string = bytes.iterator.map { b => "%02x".format(b & 0xff) }.mkString
      JString(string)
    case t @ SoQLPolygon(value) =>
      val bytes = t.typ.WkbRep(value)
      val string = bytes.iterator.map { b => "%02x".format(b & 0xff) }.mkString
      JString(string)
    case t @ SoQLMultiPolygon(value) =>
      val bytes = t.typ.WkbRep(value)
      val string = bytes.iterator.map { b => "%02x".format(b & 0xff) }.mkString
      JString(string)
    case _ => ???
  }
}
