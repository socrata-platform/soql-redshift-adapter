package com.socrata.service

import com.socrata.soql.environment.ColumnName
import com.socrata.common.utils.managed.ManagedUtils
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.id._
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Test
import org.joda.time.DateTime
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types._
import com.socrata.store.service.RedshiftSecondary

@QuarkusTest class RedshiftSecondaryTest() {
  @Inject var secondary: RedshiftSecondary = _

  val datasetInfo = DatasetInfo(
    "dataset_info",
    "locale_name",
    Array.empty,
    Some("resource_name")
  )
  val copyInfo =
    CopyInfo(new CopyId(0), 0, LifecycleStage.Published, 18, 18, DateTime.now())

  val schema = Map[ColumnId, ColumnInfo[SoQLType]](
    new ColumnId(14) ->
      ColumnInfo(
        new ColumnId(14),
        new UserColumnId("some text"),
        Some(ColumnName("Field name of text column")),
        SoQLText,
        false,
        false,
        false,
        None
      ),
    new ColumnId(33) ->
      ColumnInfo(
        new ColumnId(33),
        new UserColumnId("some boolean"),
        Some(ColumnName("Field name of boolean column")),
        SoQLBoolean,
        false,
        false,
        false,
        None
      )
  )

  val rows = ManagedUtils.construct(
    Iterator(
      ColumnIdMap.apply(
        Map(
          new ColumnId(33) -> SoQLBoolean(false),
          new ColumnId(14) -> SoQLText("hey there")
        )
      )
    )
  )

  @Test def resync(): Unit = {
    secondary.resync(
      datasetInfo,
      copyInfo,
      ColumnIdMap.apply(schema),
      None,
      rows,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      true
    )

  }
}
