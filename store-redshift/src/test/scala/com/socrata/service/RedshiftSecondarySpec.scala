package com.socrata.service

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

  val schema = Map(
    new ColumnId(0) ->
      ColumnInfo[SoQLType](
        new ColumnId(1),
        new UserColumnId("some text"),
        None,
        SoQLText,
        false,
        false,
        false,
        None
      ),
    new ColumnId(0) ->
      ColumnInfo[SoQLType](
        new ColumnId(2),
        new UserColumnId("some boolean"),
        None,
        SoQLBoolean,
        false,
        false,
        false,
        None
      )
  )

  val rows = List(ColumnIdMap.apply(
    Map(
      new ColumnId(0) -> SoQLText("hey there"),
      new ColumnId(1) -> SoQLBoolean(false)
    )
  ))

  val managedRows: Managed[Iterator[ColumnIdMap[SoQLValue]]] =
    new Managed[Iterator[ColumnIdMap[SoQLValue]]] {
      def run[B](f: Iterator[ColumnIdMap[SoQLValue]] => B): B = {
        f(rows.iterator)
      }
    }

  @Test def resync(): Unit = {
    secondary.resync(
      datasetInfo,
      copyInfo,
      ColumnIdMap.apply(schema),
      None,
      managedRows,
      Seq.empty,
      Seq.empty,
      Seq.empty,
      true
    )

  }
}
