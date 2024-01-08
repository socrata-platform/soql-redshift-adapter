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
    rewriter: SoQLRewriter,
    @DataSource("store")
    storeDataSource: AgroalDataSource
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

    val rewrittenAnalysis = rewriter.rewrite(analysis)

    val sql = Using.resource(storeDataSource.getConnection) { conn =>
      val cpp = CryptProviderProvider.empty
      val extraContext = new SoQLExtraContext(context, cpp, RedshiftSqlUtils.escapeString(conn))

      RedshiftSqlizer.apply(rewrittenAnalysis, extraContext).right.get.sql
    }

    Response.ok(Map(
      "sql" -> sql,
      "analysis" -> analysis.statement.debugStr,
      "context" -> context,
      "passes" -> passes,
      "debug" -> debug,
      "queryTimeout" -> queryTimeout,
      "locationSubColumns" -> locationSubcolumns
    )).build()
  }
}
