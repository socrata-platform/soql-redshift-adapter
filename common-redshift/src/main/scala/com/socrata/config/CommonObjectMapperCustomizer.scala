package com.socrata.config

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.dataformat.yaml.{YAMLFactory, YAMLGenerator}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}
import com.socrata.converter.FiniteDurationDeserializer
import io.quarkus.jackson.ObjectMapperCustomizer

import scala.concurrent.duration.FiniteDuration

object CommonObjectMapperCustomizer {

  def customize(objectMapper: ObjectMapper): Unit = {
    val simpleModule = new SimpleModule()
    simpleModule.addDeserializer(classOf[FiniteDuration], new FiniteDurationDeserializer)
    objectMapper
      .registerModule(new JavaTimeModule())
      .registerModule(DefaultScalaModule)
      .registerModule(simpleModule)
      .configure(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES, true)
      .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY) :: ClassTagExtensions
  }

  lazy val Default = {
    val objectMapper = new ObjectMapper()
    objectMapper.findAndRegisterModules()
    customize(objectMapper)
    objectMapper
  }

  lazy val Yaml = {
    val objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES))
    objectMapper.findAndRegisterModules()
    customize(objectMapper)
    objectMapper
  }
}

//This class is used to configure the applications ObjectMapper, the jackson serde, to be friendlier with scala.
class CommonObjectMapperCustomizer extends ObjectMapperCustomizer {
  override def customize(objectMapper: ObjectMapper): Unit = {
    CommonObjectMapperCustomizer.customize(objectMapper)
  }
}
