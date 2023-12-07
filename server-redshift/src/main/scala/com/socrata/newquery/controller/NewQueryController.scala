package com.socrata.newquery.controller

import com.socrata.analyzer2.{Deserializer, Lazy}
import com.socrata.analyzer2.Deserializer.Request
import com.socrata.newquery.api.NewQueryEndpoint
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.core.Response

import java.io.InputStream

@ApplicationScoped
class NewQueryController
(

) extends NewQueryEndpoint{
  override def post(body: InputStream): Response = {
    import com.socrata.analyzer2.metatypes.InputMetaTypes.DebugHelper._
   val parsed:Request = Deserializer(body)
    Response.ok(Map(
      "analysis"->parsed.analysis.statement.debugStr,
      "context"->parsed.context,
      "passes"->parsed.passes,
      "debug"->parsed.debug,
      "queryTimeout"->parsed.queryTimeout,
      "locationSubColumns"->parsed.locationSubcolumns
    )).build()
  }
}
