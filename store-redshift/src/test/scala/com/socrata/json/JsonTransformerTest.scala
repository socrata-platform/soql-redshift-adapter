package com.socrata.store.json

import com.socrata.common.utils.managed.ManagedUtils
import org.joda.time.format.{DateTimeFormat}
import com.vividsolutions.jts.geom.{
  LineString,
  LinearRing,
  MultiLineString,
  MultiPoint,
  MultiPolygon,
  Point,
  Polygon,
  Coordinate,
  PrecisionModel
}
import com.rojoma.simplearm.v2.Managed
import com.rojoma.json.v3.interpolation._
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.id._
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.joda.time.DateTime
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types._
import com.socrata.soql.environment.ColumnName

@QuarkusTest class JsonTransformerTest() {
  // try to make these not vars
  @Inject var jsonTransformer: JsonTransformer = _

  val schema = Map[ColumnId, ColumnInfo[SoQLType]](
    new ColumnId(2) ->
      ColumnInfo(
        new ColumnId(0),
        new UserColumnId("some text"),
        Some(ColumnName("Field name of text column")),
        SoQLText,
        false,
        false,
        false,
        None
      ),
    new ColumnId(8) ->
      ColumnInfo(
        new ColumnId(0),
        new UserColumnId("some boolean"),
        Some(ColumnName("Field name of boolean column")),
        SoQLBoolean,
        false,
        false,
        false,
        None
      ),
    new ColumnId(15) ->
      ColumnInfo(
        new ColumnId(0),
        new UserColumnId("some number"),
        Some(ColumnName("Field name of number column")),
        SoQLNumber,
        false,
        false,
        false,
        None
      ),
    new ColumnId(-2) ->
      ColumnInfo(
        new ColumnId(0),
        new UserColumnId("some point"),
        Some(ColumnName("Field name of point column")),
        SoQLPoint,
        false,
        false,
        false,
        None
      ),
    new ColumnId(-33) ->
      ColumnInfo(
        new ColumnId(0),
        new UserColumnId("some floating time stamp"),
        Some(ColumnName("Field name of floating timestamp column")),
        SoQLPoint,
        false,
        false,
        false,
        None
      )
  )

  val rows = Iterator(
    ColumnIdMap.apply(
      Map(
        new ColumnId(2) -> SoQLText("first row"),
        new ColumnId(8) -> SoQLBoolean(false),
        new ColumnId(15) -> SoQLNumber(new java.math.BigDecimal(13)),
        new ColumnId(-2) -> SoQLPoint(new Point(new Coordinate(100, 999), new PrecisionModel(), 4326))
      )
    ),
    ColumnIdMap.apply(
      Map(
        new ColumnId(2) -> SoQLText("second row"),
        new ColumnId(8) -> SoQLBoolean(true),
        new ColumnId(15) -> SoQLNumber(new java.math.BigDecimal(22)),
        new ColumnId(-33) -> SoQLFloatingTimestamp(
          DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-sszzz").parseLocalDateTime("2021-06-13 18-14-23CST")
        )
      )
    )
  )

  @Test def test: Unit = {
    assertEquals(
      List(
        j"""{
  "field_name_of_point_column_0" : "00000000014059000000000000408f380000000000",
  "field_name_of_text_column_0" : "first row",
  "field_name_of_boolean_column_0" : false,
  "field_name_of_number_column_0": 13
}""",
        j"""{
  "field_name_of_boolean_column_0" : true,
  "field_name_of_floating_timestamp_column_0" : "2021-06-13 18:14:23",
  "field_name_of_number_column_0": 22,
  "field_name_of_text_column_0" : "second row"
}"""
      ),
      jsonTransformer.transformAll(rows, ColumnIdMap.apply(schema)).toList
    )
  }

}
