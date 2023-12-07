package com.socrata.newquery.api

import jakarta.ws.rs.core.{MediaType, Response}
import jakarta.ws.rs.{Consumes, POST, Path, Produces}

import java.io.InputStream

@Path("/new-query")
trait NewQueryEndpoint {

  @Consumes(Array(MediaType.APPLICATION_OCTET_STREAM))
  @Produces(Array("application/x-socrata-gzipped-cjson"))
  @POST def post(body: InputStream): Response

}
