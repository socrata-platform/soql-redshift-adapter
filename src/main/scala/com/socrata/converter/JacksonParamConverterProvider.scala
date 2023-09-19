package com.socrata.converter

import com.fasterxml.jackson.databind.ObjectMapper
import com.rojoma.json.v3.ast.{JString, JValue}
import com.socrata.datacoordinator.id.RollupName
import com.socrata.soql.stdlib.Context
import jakarta.ws.rs.ext.{ParamConverter, ParamConverterProvider, Provider}
import org.joda.time.DateTime

import java.lang.annotation.Annotation
import java.lang.reflect.Type

class JacksonParamConverterProvider(objectMapper:ObjectMapper) extends ParamConverterProvider{
  override def getConverter[T](rawType: Class[T], genericType: Type, annotations: Array[Annotation]): ParamConverter[T] = {
    jacksonMarshall(rawType)
  }

  def jacksonMarshall[T](rawType: Class[T]):ParamConverter[T]={
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
