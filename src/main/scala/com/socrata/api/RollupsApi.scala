package com.socrata.api

import com.socrata.datacoordinator.truth.metadata.UnanchoredRollupInfo
import io.smallrye.mutiny.Multi
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.{BeanParam, GET, Path, Produces}
import org.jboss.resteasy.reactive.RestQuery

import java.util.Optional
import scala.annotation.meta.field

case class RollupsRequest() {
  @(RestQuery@field)("include_unmaterialized") var includeUnmaterialized: Optional[java.lang.Boolean] = Optional.empty()
  @(RestQuery@field) var copy: Optional[String] = Optional.empty()
  @(RestQuery@field) var ds: Optional[String] = Optional.empty()
  @(RestQuery@field) var rn: Optional[String] = Optional.empty()
}

trait RollupsResponse {

}

@Path("/rollups")
trait RollupsApi {
  @GET
  @Produces(Array[String](MediaType.APPLICATION_JSON))
  def getRollups(@BeanParam rollupsRequest: RollupsRequest): Multi[UnanchoredRollupInfo]
}
