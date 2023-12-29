package com.socrata.store.json

import org.joda.time.format.{DateTimeFormat}
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
case object JsonTransformerImpl extends JsonTransformer {
  def transform(colIdMap: ColumnIdMap[SoQLValue], schema: ColumnIdMap[ColumnInfo[SoQLType]]): JValue = {
    val dbRow: Map[String, SoQLValue] = colIdMap.foldLeft(Map.empty[String, SoQLValue]) { case (state, (id, value)) =>
      state.updated(ColumnNames.name(schema(id)), value)
    }
    JObject(dbRow.mapValues(transformValue))
  }

  private def transformValue(soqlValue: SoQLValue): JValue = {

    /*
     offset is not being produced or respected, it seems.

     */

    val dateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssZ")
    soqlValue match {
      case SoQLText(value) => JString(value)
      case SoQLNumber(value) => JNumber(value)
      case SoQLBoolean(value) => JBoolean(value)
      case t @ SoQLFixedTimestamp(value) => JString(dateTimeFormat.print(value))
      case t @ SoQLFloatingTimestamp(value) => JString(dateTimeFormat.print(value))
      case t @ SoQLDate(value) => JString(dateTimeFormat.print(value))
      case t @ SoQLTime(value) => JString(dateTimeFormat.print(value))
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
}
