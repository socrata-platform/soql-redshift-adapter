package com.socrata.config

import com.fasterxml.jackson.databind.JsonNode

import scala.io.Source

case class JacksonYamlConfigSource(resource: String) extends ConfigSource {
  override def read(): JsonNode =
    CommonObjectMapperCustomizer.Yaml.readValue(Source.fromResource(resource).mkString, classOf[JsonNode])
}
