package com.socrata.db.meta.service

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
import com.socrata.db.meta.entity._
import com.socrata.soql.environment.ColumnName

@QuarkusTest class DatasetServiceSpec() {
  @Inject var svc: DatasetService = _

  @Test def test: Unit = {
    val internalName: String = "alpha50"
    val copyNumber: Long = 20L
    val datasetInfo: DatasetInfo = DatasetInfo(internalName, "en", Array.empty, Some("aaaa-aaaa"))
    val copyInfo: CopyInfo = CopyInfo(new CopyId(10), copyNumber, LifecycleStage.Published, 1L, 1L, DateTime.now())
    val dataset: Dataset = Dataset(datasetInfo, copyInfo)
    println("================================================================", svc.persist(dataset))
    println("================================================================", svc.persist(dataset))
  }

}
