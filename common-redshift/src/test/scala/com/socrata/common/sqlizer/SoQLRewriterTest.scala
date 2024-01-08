package com.socrata.common.sqlizer

import com.socrata.prettyprint.prelude._
import com.socrata.soql.types._
import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions._
import com.socrata.soql.sqlizer._

import com.socrata.soql.environment.Provenance

import io.quarkus.test.junit.QuarkusTest
import org.junit.jupiter.api.{Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test;
import com.socrata.common.sqlizer._
import com.socrata.common.sqlizer.metatypes._

object SoQLRewriterTest {

  val analyzer =
    new SoQLAnalyzer[DatabaseNamesMetaTypes](
      new SoQLTypeInfo2,
      SoQLFunctionInfo,
      SoQLSqlizerTest.ProvenanceMapper
    )

}

@QuarkusTest
class SoQLRewriterTest {

  @Test
  def test(): Unit = {}
}
