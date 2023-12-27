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
    new ColumnId(0) ->
      ColumnInfo(
        new ColumnId(1),
        new UserColumnId("some text"),
        Some(ColumnName("Field name of text column")),
        SoQLText,
        false,
        false,
        false,
        None
      ),
    new ColumnId(1) ->
      ColumnInfo(
        new ColumnId(2),
        new UserColumnId("some boolean"),
        Some(ColumnName("Field name of boolean column")),
        SoQLBoolean,
        false,
        false,
        false,
        None
      )
  )

  val rows = Iterator(
    ColumnIdMap.apply(
      Map(
        new ColumnId(0) -> SoQLText("first row"),
        new ColumnId(1) -> SoQLBoolean(false)
      )
    ),
    ColumnIdMap.apply(
      Map(
        new ColumnId(0) -> SoQLText("second row"),
        new ColumnId(1) -> SoQLBoolean(true)
      )
    )
  )

  @Test def test: Unit = {
    assertEquals(
      jsonTransformer.transformAll(rows, ColumnIdMap.apply(schema)).toList,
      List(
        j"""{
  "field_name_of_boolean_column" : false,
  "field_name_of_text_column" : "first row"
}""",
        j"""{
  "field_name_of_boolean_column" : true,
  "field_name_of_text_column" : "second row"
}"""
      )
    )
  }

}
