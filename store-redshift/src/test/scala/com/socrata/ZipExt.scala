package com.socrata.store.sqlizer

import scala.util._
import com.socrata.util.ResultSet
import com.socrata.common.sqlizer.metatypes._

import com.socrata.store._
import com.vividsolutions.jts.geom.{LineString, LinearRing, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon, Coordinate, PrecisionModel}
import com.socrata.soql.parsing._
import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.interpolation._

import com.socrata.prettyprint.prelude._
import com.socrata.soql.types._
import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions._
import com.socrata.soql.sqlizer._

import com.typesafe.config.ConfigFactory
import com.socrata.datacoordinator.common._
import com.socrata.datacoordinator.secondary.DatasetInfo
import com.socrata.soql.environment.Provenance


import io.quarkus.logging.Log
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.{DisplayName, Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test;
import com.socrata.common.sqlizer._

import TableCreationTest.hasType

import org.joda.time.{DateTime, LocalDate, LocalDateTime, LocalTime, Period}
import org.joda.time.format.{DateTimeFormat}

object ZipExt {
  implicit class ZipUtils[T](seq: Seq[T]) {
    def zipExact[R](other: Seq[R]): Seq[(T, R)] = {
      if(seq.length == other.length) {
        seq.zip(other)
      } else fail(s"""You cannot pass...${seq} and ${other} are different lengths
The dark fire will not avail you, flame of Udûn. """)
    }
  }
}
