package com.socrata.api

import com.socrata.datacoordinator.truth.metadata.SchemaWithFieldName
import io.smallrye.mutiny.Uni
import jakarta.ws.rs._
import jakarta.ws.rs.core.MediaType
import org.jboss.resteasy.reactive.RestQuery

import java.util.Optional
import scala.annotation.meta.field


case class SchemaRequest() {
  @(RestQuery@field) var copy: String = null
  @(RestQuery@field) var fieldName: Optional[java.lang.Boolean] = Optional.empty()
  @(RestQuery@field) var ds: Optional[String] = Optional.empty()
  @(RestQuery@field) var rn: Optional[String] = Optional.empty()
}


@Path("/schema")
trait SchemaApi {
  @GET
  @Produces(Array[String](MediaType.APPLICATION_JSON))
  def getSchema(@BeanParam schemaRequest: SchemaRequest): Uni[SchemaWithFieldName]
}
