package com.socrata.db.meta.service

import com.socrata.common.utils.managed.ManagedUtils
import org.joda.time.format.DateTimeFormat
import com.vividsolutions.jts.geom.{Coordinate, LineString, LinearRing, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon, PrecisionModel}
import com.rojoma.simplearm.v2.Managed
import com.rojoma.json.v3.interpolation._
import com.socrata.common.db.meta.entity.Dataset
import com.socrata.common.db.meta.service.DatasetService
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

@QuarkusTest class DatasetServiceSpec() {
  @Inject var svc: DatasetService = _

  @Test def test: Unit = {
    val dataset: Dataset = Dataset(
      DatasetInfo("alpha50", "en", Array.empty, Some("aaaa-aaaa")),
      CopyInfo(new CopyId(10), 20L, LifecycleStage.Published, 1L, 1L, DateTime.now())
    )
    println("================================================================", svc.persist(dataset))
    val newDataset = Dataset(
      DatasetInfo("alpha50", "en", Array.empty, Some("aaaa-aaaa")),
      CopyInfo(new CopyId(10), 20L, LifecycleStage.Published, 1L, 1L, DateTime.now())
    )
    newDataset.table = "foobarbaz"
    println("================================================================", svc.persist(newDataset))

    val dataset2: Dataset = Dataset(
      DatasetInfo("alpha51", "en", Array.empty, Some("aaaa-aaaa")),
      CopyInfo(new CopyId(10), 20L, LifecycleStage.Published, 1L, 1L, DateTime.now())
    )
    println("================================================================", svc.persist(dataset2))
    val newDataset2 = Dataset(
      DatasetInfo("alpha51", "en", Array.empty, Some("aaaa-aaaa")),
      CopyInfo(new CopyId(10), 20L, LifecycleStage.Published, 1L, 1L, DateTime.now())
    )
    newDataset2.table = "foobarbaz"
    println("================================================================", svc.persist(newDataset2))

    println(
      "================================================================",
      svc.persist(Dataset(
        DatasetInfo("alpha12", "en", Array.empty, Some("aaaa-aaaa")),
        CopyInfo(new CopyId(10), 20L, LifecycleStage.Published, 1L, 1L, DateTime.now())
      ))
    )

    println(
      "================================================================",
      svc.persist(Dataset(
        DatasetInfo("alpha12", "en", Array.empty, Some("aaaa-aaaa")),
        CopyInfo(new CopyId(10), 20L, LifecycleStage.Published, 1L, 1L, DateTime.now())
      ))
    )

    println(
      "================================================================",
      svc.persist(Dataset(
        DatasetInfo("alpha13", "en", Array.empty, Some("aaaa-aaaa")),
        CopyInfo(new CopyId(10), 20L, LifecycleStage.Published, 1L, 1L, DateTime.now())
      ))
    )

  }

}
