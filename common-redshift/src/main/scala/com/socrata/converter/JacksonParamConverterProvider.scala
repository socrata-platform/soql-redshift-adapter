package com.socrata.converter

import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.ws.rs.ext.{ParamConverter, ParamConverterProvider}

import java.lang.annotation.Annotation
import java.lang.reflect.Type

//This makes jackson handle parameter marshalling, but there are complications. We don't need this now/yet.
//@ApplicationScoped
class JacksonParamConverterProvider(objectMapper: ObjectMapper) extends ParamConverterProvider {
  override def getConverter[T](rawType: Class[T], genericType: Type, annotations: Array[Annotation]): ParamConverter[T] = {
    jacksonMarshall(rawType)
  }

  def jacksonMarshall[T](rawType: Class[T]): ParamConverter[T] = {
    new ParamConverter[T] {
      override def fromString(value: String): T = {
        objectMapper.readValue(value, rawType)
      }

      override def toString(value: T): String = {
        objectMapper.writeValueAsString(value)
      }
    }
  }
}
