package com.socrata.service

import com.socrata.soql.environment.ColumnName
import com.socrata.common.utils.managed.ManagedUtils
import com.rojoma.simplearm.v2.Managed
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.id._
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Test
import org.joda.time.DateTime
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types._

@QuarkusTest class RedshiftSecondaryTest() {
  @Inject var secondary: RedshiftSecondary = _

  val datasetInfo = DatasetInfo("dataset_info", "locale_name", Array.empty, Some("resource_name"))
  val copyInfo = CopyInfo(new CopyId(0), 0, LifecycleStage.Published, 18, 18, DateTime.now())

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

  val rows = ManagedUtils.construct(Iterator(
    ColumnIdMap.apply(Map(
      new ColumnId(0) -> SoQLText("hey there"),
      new ColumnId(1) -> SoQLBoolean(false)
    ))
  ))

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

/*

 make Transactional work (failures should do nothing to the DB).
 Make ID auto increment
 Write Columns.

 persist should complain if thing already exists

 */
