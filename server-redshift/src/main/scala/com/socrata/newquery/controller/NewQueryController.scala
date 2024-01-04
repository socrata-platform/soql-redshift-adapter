package com.socrata.newquery.controller

import com.socrata.soql.analyzer2._
import com.socrata.common.sqlizer.{metatypes, _}
import com.socrata.newquery.api.NewQueryEndpoint
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.core.Response

import java.io.InputStream

@ApplicationScoped
class NewQueryController(
) extends NewQueryEndpoint {
  override def post(body: InputStream): Response = {
    import metatypes.InputMetaTypes.DebugHelper._
    implicit def cpp = CryptProviderProvider.empty

    val Deserializer.Request(
      analysis: SoQLAnalysis[metatypes.InputMetaTypes],
      context,
      passes,
      debug,
      queryTimeout,
      locationSubcolumns
    ) =
      Deserializer(body)

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
