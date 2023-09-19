package com.socrata.api

import com.fasterxml.jackson.annotation.{JsonAutoDetect, JsonInclude, JsonProperty, JsonTypeName}
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import io.smallrye.mutiny.Uni
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.{GET, Path, Produces}

import scala.annotation.meta.field

@JsonInclude
trait VersionResponse {
  @JsonProperty
  def version: String
}

@Path("/version")
trait VersionApi {
  @GET
  @Produces(Array[String](MediaType.APPLICATION_JSON))
  def getVersion(): Uni[VersionResponse]
}
