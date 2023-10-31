package com.socrata.config

import scala.jdk.CollectionConverters._
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}
import io.quarkus.jackson.ObjectMapperCustomizer
import jakarta.inject.Singleton
import org.eclipse.microprofile.config.Config

object JacksonConfig {

  def customize(objectMapper: ObjectMapper): Unit = {
    objectMapper
      .registerModule(DefaultScalaModule)
      .configure(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES, false)
      .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
      .setVisibility(PropertyAccessor.FIELD, Visibility.ANY) :: ClassTagExtensions
  }

  def deserializeConfig(config:Config,prefix:String): JsonNode ={
    val rootNode = Default.createObjectNode()
    val prefixDot = if (prefix.isEmpty) prefix else s"$prefix."
    config.getPropertyNames.forEach{name=>
      if(name.startsWith(prefixDot)){
        val parts = name.substring(prefixDot.length).split("\\.")
        var currentNode = rootNode
        for(i <- 0 until parts.length -1){
          currentNode=currentNode.`with`(parts(i))
        }
        currentNode.put(parts(parts.length-1),config.getValue(name,classOf[String]))
      }
    }
    rootNode
  }

  //A default singelton instance of our configured ObjectMapper
  lazy val Default = {
    val objectMapper = new ObjectMapper()
    customize(objectMapper)
    objectMapper
  }
}

//This class is used to configure the applications ObjectMapper, the jackson serde, to be friendlier with scala.
class JacksonConfig extends ObjectMapperCustomizer {
  override def customize(objectMapper: ObjectMapper): Unit = {
    JacksonConfig.customize(objectMapper)
  }
}
