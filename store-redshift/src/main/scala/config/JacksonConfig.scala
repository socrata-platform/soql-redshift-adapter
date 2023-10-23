package config

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}
import io.quarkus.jackson.ObjectMapperCustomizer
import jakarta.inject.Singleton

object JacksonConfig {

  def customize(objectMapper: ObjectMapper): Unit = {
    objectMapper
      .registerModule(DefaultScalaModule)
      .configure(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES, true)
      .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY) :: ClassTagExtensions
  }
}

//This class is used to configure the applications ObjectMapper, the jackson serde, to be friendlier with scala.
@Singleton
class JacksonConfig extends ObjectMapperCustomizer {
  override def customize(objectMapper: ObjectMapper): Unit = {
    JacksonConfig.customize(objectMapper)
  }
}
