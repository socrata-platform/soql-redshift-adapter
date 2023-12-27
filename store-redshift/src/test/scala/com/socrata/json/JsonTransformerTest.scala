package com.socrata.store.json

import com.socrata.common.utils.managed.ManagedUtils
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
      )
  )

  val rows = Iterator(
    ColumnIdMap.apply(
      Map(
        new ColumnId(2) -> SoQLText("first row"),
        new ColumnId(8) -> SoQLBoolean(false),
        new ColumnId(15) -> SoQLNumber(new java.math.BigDecimal(13))
      )
    ),
    ColumnIdMap.apply(
      Map(
        new ColumnId(2) -> SoQLText("second row"),
        new ColumnId(8) -> SoQLBoolean(true),
        new ColumnId(15) -> SoQLNumber(new java.math.BigDecimal(22))
      )
    )
  )

  @Test def test: Unit = {
    assertEquals(
      jsonTransformer.transformAll(rows, ColumnIdMap.apply(schema)).toList,
      List(
        j"""{
  "field_name_of_text_column_0" : "first row",
  "field_name_of_boolean_column_0" : false,
  "field_name_of_number_column_0": 13
}""",
        j"""{
  "field_name_of_boolean_column_0" : true,
  "field_name_of_number_column_0": 22,
  "field_name_of_text_column_0" : "second row"
}"""
      )
    )
  }

}
