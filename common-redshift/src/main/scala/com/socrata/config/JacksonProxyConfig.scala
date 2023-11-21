package com.socrata.config

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.socrata.config.JacksonProxyConfigBuilder.merge

import java.lang.reflect.{InvocationHandler, Method, ParameterizedType, Proxy}
import com.fasterxml.jackson.databind.PropertyNamingStrategies

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

object JacksonProxyConfigBuilder{

  //Merges ConfigSources into a JsonNode (folding), taking the starting node
   def merge(configSources: Seq[ConfigSource], start: JsonNode): JsonNode = {
    configSources.foldLeft(
      start
    )(
      (acc, item) => merge(acc, item.read())
    )
  }

  //Merges two JsonNodes
   def merge(mainNode: JsonNode, updateNode: JsonNode): JsonNode = {
    val fieldNames = updateNode.fieldNames
    while (fieldNames.hasNext) {
      val fieldName = fieldNames.next
      val jsonNode = mainNode.get(fieldName)
      if (jsonNode != null && jsonNode.isObject) merge(jsonNode, updateNode.get(fieldName))
      else mainNode match {
        case node: ObjectNode =>
          val value = updateNode.get(fieldName)
          node.replace(fieldName, value)
        case _ =>
      }
    }
    mainNode
  }
}

trait ConfigSource {
  def read(): JsonNode
}

trait ConfigProvider{
  //Get a prefixed config interface/trait.
  def proxy[T](path: String, clazz: Class[T]): T
  //Use the root to get the config interface/trait.
  def proxy[T](clazz: Class[T]): T
  //Once you chunk, you lose access to ConfigBuilder and cannot source further.
  def chunk(path:String): ConfigProvider
}

trait ConfigBuilder{
  //Allows you to either continue adding sources, or complete the proxying via the provider
  def withSources(configSources: ConfigSource*): ConfigBuilder with ConfigProvider
}

case class JacksonProxyConfigBuilder(private val objectMapper:ObjectMapper) extends ConfigBuilder {
  def withSources(configSources: ConfigSource*): JacksonProxyConfigProvider =
    JacksonProxyConfigProvider(
      //Merges config sources into a JsonNode, starting from a blank empty JsonNode
      merge(configSources,objectMapper.createObjectNode().asInstanceOf[JsonNode]),
      objectMapper
    )
}

//Uses Jacksons to delegate method calls of a proxy, backed by a JsonNode. Uses an ObjectMapper to convert/marshall stuff.
case class JsonNodeBackedJacksonInvocationHandler(data: JsonNode, objectMapper:ObjectMapper) extends InvocationHandler {
  def invoke(proxy: scala.AnyRef, method: Method, args: Array[AnyRef]): AnyRef = {
    method.getName match{
      //Simple implementation for toString, when the root interface is accessed directly.
      case "toString" => data.toString //Could also make it pretty?
      //implement others when it becomes important...ie equals/hash...not sure this matters much for our configs yet?
      case methodName => data.findValue(PropertyNamingStrategies.KebabCaseStrategy.INSTANCE.translate(methodName)) match {
        case null => throw new NotImplementedError(s"Unsupported method name '$methodName', implement it above (probably)!")
        //Its an object, so instead of using reflection - lets also construct a proxy for this child. This lets us use nested interfaces, neat.
        case value: ObjectNode => method.getReturnType.getName match {
          //TODO Option of simple vs Option of complex, later check if target class is interface or not, rather than try.
          case "scala.Option" => Try(JacksonProxyConfigProvider(value, objectMapper).proxy(Class.forName(method.getGenericReturnType.asInstanceOf[ParameterizedType].getActualTypeArguments.head.getTypeName)).asInstanceOf[AnyRef]) match {
            case Failure(exception) => Try(objectMapper.readValue(
              value.toString,
              Class.forName(method.getGenericReturnType.asInstanceOf[ParameterizedType].getActualTypeArguments.head.getTypeName)
            ).asInstanceOf[AnyRef]) match {
              case Failure(exception) => None
              case Success(value) => Some(value)
            }
            case Success(value) => Some(value)
          }
          case _ => JacksonProxyConfigProvider(value, objectMapper).proxy(method.getReturnType).asInstanceOf[AnyRef]
        }
        case value: ArrayNode => value.elements().asScala.toList.map {
          //TODO Array item of simple vs Array item of complex, same here, later check if target class is interface or not, rather than try
          case item: ObjectNode => Try(JacksonProxyConfigProvider(item, objectMapper).proxy(Class.forName(method.getGenericReturnType.asInstanceOf[ParameterizedType].getActualTypeArguments.head.getTypeName)).asInstanceOf[AnyRef]) match {
            case Failure(_) => objectMapper.readValue(
              item.toString,
              Class.forName(method.getGenericReturnType.asInstanceOf[ParameterizedType].getActualTypeArguments.head.getTypeName)
            ).asInstanceOf[AnyRef]
            case Success(value) =>  value
          }
          case item => objectMapper.readValue(
            item.toString,
            Class.forName(method.getGenericReturnType.asInstanceOf[ParameterizedType].getActualTypeArguments.head.getTypeName)
          ).asInstanceOf[AnyRef]
        }
        //Convert the JsonNode object to something typed, like a TextNode to a String...etc
        case value => objectMapper.readValue(
          value.toString,
          method.getReturnType
        ).asInstanceOf[AnyRef]
      }
    }
  }
}

case class JacksonProxyConfigProvider(private val data: JsonNode, private val objectMapper:ObjectMapper) extends ConfigProvider with ConfigBuilder {

  def withSources(configSources: ConfigSource*): JacksonProxyConfigProvider =
    JacksonProxyConfigProvider(
      //Merge new sources with existing JsonNode
      merge(configSources,data),
      objectMapper
    )

  def chunk(path:String): JacksonProxyConfigProvider = JacksonProxyConfigProvider(data.findValue(path),objectMapper)

  private def proxy[T](clazz: Class[T], newData: JsonNode): T = {
    //We will return a proxy that will delegate method calls that are backed by a JsonNode and Jackson
    Proxy.newProxyInstance(
      //Use the classLoader of the target class?
      clazz.getClassLoader,
      //This anonymous proxy will only be implementing a single interface/trait (which is our target marshalling class)
      Array(clazz),
      //Uses Jackson to delegate method calls backed by a JsonNode
      JsonNodeBackedJacksonInvocationHandler(
        newData,
        objectMapper)
      //Our proxy will implement our target interface/trait (clazz), so we will force cast it to look like this type
    ).asInstanceOf[T]
  }

  def proxy[T](path: String, clazz: Class[T]): T = {
    //First extract the value at the json path from the root node, then use this as the proxy data
    proxy(clazz, data.findValue(path))
  }

  def proxy[T](clazz: Class[T]): T = {
    //Use the root data as the proxy data
    proxy(clazz, data)
  }
}
