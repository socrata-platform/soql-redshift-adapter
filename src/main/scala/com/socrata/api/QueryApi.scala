package com.socrata.api

import com.fasterxml.jackson.annotation.JsonProperty
import io.smallrye.mutiny.Multi
import jakarta.enterprise.inject.build.compatible.spi.Validation
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs._
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType
import org.eclipse.microprofile.openapi.annotations.media.{DiscriminatorMapping, Schema}
import org.jboss.resteasy.reactive.{RestHeader, RestQuery}

import java.time.LocalDateTime
import java.util.Optional
import scala.annotation.meta.field


@Validation
case class QueryRequest() {
  @(RestQuery@field) var context: Optional[String] = Optional.empty()
  @(RestQuery@field) var dataset: Optional[String] = Optional.empty()
  @(RestQuery@field) var query: Optional[String] = Optional.empty()
  @(RestQuery@field) var rowCount: Optional[String] = Optional.empty()
  @(RestQuery@field) var copy: Optional[String] = Optional.empty()
  @(RestQuery@field) var rollupName: Optional[String] = Optional.empty()
  @(RestQuery@field) var obfuscateId: Optional[java.lang.Boolean] = Optional.empty()
  @(RestQuery@field) var queryTimeoutSeconds: Optional[java.lang.Long] = Optional.empty()
  @(RestQuery@field)("X-Socrata-Debug") var debug: Optional[java.lang.Boolean] = Optional.empty()
  @(RestHeader@field)("X-Socrata-Analyze") var analyze: Optional[java.lang.Boolean] = Optional.empty()
  @(RestHeader@field)("If-Modified-Since") var modifiedSince: Optional[LocalDateTime] = Optional.empty()
  @(RestHeader@field)("X-Socrata-Last-Modified") var modifiedLast: Optional[LocalDateTime] = Optional.empty()
}


@Path("/query")
trait QueryApi {


  type Id = java.lang.Long

  @GET
  @Produces(Array[String](MediaType.APPLICATION_JSON))
  def getQuery(@BeanParam queryRequest: QueryRequest): Multi[Row]

  @POST
  @Produces(Array[String](MediaType.APPLICATION_JSON))
  def postQuery(@BeanParam queryRequest: QueryRequest): Multi[Row]

  trait Column {
    @JsonProperty
    def id: Id

    @JsonProperty
    def value: Value
  }

  trait Row {
    @JsonProperty
    def columns: Array[Column]
  }

  @Schema(
    `type` = SchemaType.OBJECT,
    title = "Value",
    anyOf = Array(classOf[TextValue],classOf[NumberValue]),
    discriminatorProperty = "valueType",
    discriminatorMapping = Array(
      new DiscriminatorMapping(value = "Text",schema = classOf[TextValue]),
      new DiscriminatorMapping(value = "Number",schema = classOf[NumberValue])
    )

  )
  sealed abstract class Value(@JsonProperty @Schema(required = true) val valueType:String)

  sealed case class TextValue(@JsonProperty value: String) extends Value("Text")
  sealed case class NumberValue(@JsonProperty value: Integer) extends Value("Number")
}
