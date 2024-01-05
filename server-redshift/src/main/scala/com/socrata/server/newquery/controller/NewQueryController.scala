package com.socrata.server.newquery.controller

import scala.util._
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource

import java.sql.Connection
import com.socrata.common.sqlizer.metatypes._
import com.socrata.soql.analyzer2._
import com.socrata.common.sqlizer.{metatypes, _}
import com.socrata.server.newquery.api.NewQueryEndpoint
import com.socrata.server.util.RedshiftSqlUtils
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.core.Response

import java.io.InputStream

@ApplicationScoped
class NewQueryController(
    @DataSource("store")
    storeDataSource: AgroalDataSource,
    databaseEntityMetaTypes: DatabaseEntityMetaTypes
) extends NewQueryEndpoint {
  override def post(body: InputStream): Response = {
    import metatypes.InputMetaTypes.DebugHelper._
    implicit def cpp = CryptProviderProvider.empty

    val Deserializer.Request(
      analysis: SoQLAnalysis[metatypes.InputMetaTypes],
      locationSubcolumns,
      context: Map[String, String],
      passes,
      debug,
      queryTimeout
    ) = Deserializer(body)

    val analysis2 = databaseEntityMetaTypes.rewriteFrom(analysis, InputMetaTypes.provenanceMapper)
    val analysis3 = DatabaseNamesMetaTypes.rewriteFrom(databaseEntityMetaTypes, analysis2)

    Using.resource(storeDataSource.getConnection) { conn =>
      val cpp = CryptProviderProvider.empty
      val extraContext = new SoQLExtraContext(context, cpp, RedshiftSqlUtils.escapeString(conn))

      val sql = RedshiftSqlizer.apply(analysis3, extraContext).right.get.sql
    }

    Response.ok(Map(
      "analysis" -> analysis.statement.debugStr,
      "context" -> context,
      "passes" -> passes,
      "debug" -> debug,
      "queryTimeout" -> queryTimeout,
      "locationSubColumns" -> locationSubcolumns
    )).build()
  }
}
