package com.socrata.query.api

import jakarta.ws.rs._
import jakarta.ws.rs.core.{MediaType, Response}

import java.io.InputStream
import java.util.Optional

class QueryEndpointPostParams {
  @QueryParam("dataset") var dataset: String = _

  @QueryParam("context") var context: String = _

  @QueryParam("schemaHash") var schemaHash: String = _

  @QueryParam("queryTimeoutSeconds") var queryTimeoutSeconds: Int = _

  @QueryParam("copy") var copy: String = _

  @QueryParam("X-Socrata-Debug") var debug: Optional[String] = _
}

@Path("/query") trait QueryEndpoint {

  @Consumes(Array(MediaType.TEXT_PLAIN))
  @Produces(Array(MediaType.TEXT_PLAIN))
  @POST def post(@BeanParam params: QueryEndpointPostParams, query: InputStream): Response
}
