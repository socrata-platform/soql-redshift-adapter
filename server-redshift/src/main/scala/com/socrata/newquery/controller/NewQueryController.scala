package com.socrata.newquery.controller

import com.socrata.analyzer2.{Deserializer}
import com.socrata.analyzer2.Deserializer.Request
import com.socrata.newquery.api.NewQueryEndpoint
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.core.Response

import com.socrata.analyzer2.metatypes.InputMetaTypes

import java.io.InputStream

@ApplicationScoped
class NewQueryController
(

) extends NewQueryEndpoint{
  override def post(body: InputStream): Response = {
    locally {
      val parsed: Request = Deserializer(body)
      Response.ok(Map(
        "context"->parsed.context,
        "passes"->parsed.passes,
        "debug"->parsed.debug,
        "queryTimeout"->parsed.queryTimeout,
        "locationSubColumns"->parsed.locationSubcolumns
      )).build()
    }
  }
}



import com.socrata.soql.analyzer2.{SoQLAnalysis}
import com.socrata.util.CryptProviderProvider
class Foo[MT <: InputMetaTypes]() {
  def parsed(body: InputStream) = {
    val parsed = Deserializer(body)
    val analysis: SoQLAnalysis[InputMetaTypes] = parsed.analysis
    locally {
      implicit def cpp = CryptProviderProvider.empty
      import InputMetaTypes.DebugHelper._
      analysis.statement.debugStr

    }
    println(analysis)
  }

}
