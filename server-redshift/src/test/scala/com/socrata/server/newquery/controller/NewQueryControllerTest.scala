package com.socrata.server.newquery.controller

import com.socrata.common.sqlizer._
import com.socrata.common.sqlizer.metatypes._
import com.socrata.common._

import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.junit.jupiter.api.{Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test;
import com.socrata.common.sqlizer._

@QuarkusTest
class NewQueryControllerTest extends TableCreationUtils {
  val repProvider = TestRepProvider
  val schema = SchemaImpl(repProvider)
  val rows = RowsImpl(repProvider)

  @Test
  def test(): Unit = {

  }
}
