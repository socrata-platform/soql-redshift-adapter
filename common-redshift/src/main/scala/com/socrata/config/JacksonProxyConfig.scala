package com.socrata.config

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.socrata.config.JacksonProxyConfigBuilder.merge

import java.lang.reflect.{InvocationHandler, Method, ParameterizedType, Proxy}
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.socrata.config.JsonNodeBackedJacksonInvocationHandler.{innerGenericClass, kebab}

import java.util.Properties
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

object JacksonProxyConfigBuilder {

  def merge(envSources: Seq[EnvSource], start: Properties): Properties = {
    envSources.foldLeft(
      start
    )((acc, item) => {
      // https://github.com/scala/bug/issues/10418
      item.read().forEach((k, v) => acc.put(k, v))
      acc
    })
  }

  // Merges ConfigSources into a JsonNode (folding), taking the starting node
  def merge(configSources: Seq[ConfigSource], start: JsonNode): JsonNode = {
    configSources.foldLeft(
      start
    )((acc, item) => merge(acc, item.read()))
  }

  // Merges two JsonNodes
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
  // Anything that can produce json
  def read(): JsonNode
}

trait EnvSource {
  // Anything that can produce json
  def read(): Properties
}

trait ConfigProvider {
  // Get a prefixed config interface/trait.
  def proxy[T](path: String, clazz: Class[T]): T
  // Use the root to get the config interface/trait.
  def proxy[T](clazz: Class[T]): T
  // Selects a subset of configs based on prefix, Once you chunk, you lose access to ConfigBuilder and cannot source further.
  def chunk(path: String): ConfigProvider
}

trait ConfigBuilder {
  // Allows you to either continue adding sources, or complete the proxying via the provider
  def withSources(configSources: ConfigSource*): ConfigBuilder with ConfigProvider
  def withEnvs(configEnvs: EnvSource*): ConfigBuilder with ConfigProvider
}

case class JacksonProxyConfigBuilder(private val objectMapper: ObjectMapper) extends ConfigBuilder {
  override def withSources(configSources: ConfigSource*): JacksonProxyConfigProvider =
    JacksonProxyConfigProvider(
      // Merges config sources into a JsonNode, starting from a blank empty JsonNode
      merge(configSources, objectMapper.createObjectNode().asInstanceOf[JsonNode]),
      objectMapper,
      new Properties()
    )

  override def withEnvs(configEnvs: EnvSource*): JacksonProxyConfigProvider =
    JacksonProxyConfigProvider(
      objectMapper.createObjectNode().asInstanceOf[JsonNode],
      objectMapper,
      merge(configEnvs, new Properties())
    )
}

object JsonNodeBackedJacksonInvocationHandler {
  def innerGenericClass(method: Method): Class[_] = {
    Class.forName(method.getGenericReturnType.asInstanceOf[ParameterizedType].getActualTypeArguments.head.getTypeName)
  }

  // We will strictly use kebab case
  def kebab(in: String): String = {
    PropertyNamingStrategies.KebabCaseStrategy.INSTANCE.translate(in)
  }
}

//Uses Jackson to delegate method calls of a proxy, backed by a JsonNode. Uses an ObjectMapper to convert/marshall stuff.
case class JsonNodeBackedJacksonInvocationHandler(data: JsonNode, objectMapper: ObjectMapper, env: Properties)
    extends InvocationHandler {
  def invoke(proxy: scala.AnyRef, method: Method, args: Array[AnyRef]): AnyRef = {
    val out = method.getName match {
      // Simple implementation for toString, when the root interface is accessed directly.
      case "toString" => data.toString
      // implement others when it becomes important...ie equals/hash...not sure this matters much for our configs yet?

      case methodName => data.findValue(kebab(methodName)) match {
          case null =>
            throw new NotImplementedError(s"Unsupported method name '$methodName', implement it above (probably)!")
          case value: ObjectNode => handleObject(value, method)
          case value: ArrayNode => handleArray(value.elements().asScala, innerGenericClass(method))
          case value => doConvert(value.toString, method.getReturnType)
        }
    }
    // Any -> AnyRef
    out.asInstanceOf[AnyRef]
  }

  private def handleObject(data: JsonNode, method: Method): Any = {
    method.getReturnType.getName match {
      // TODO Option of simple vs Option of complex, later check if target class is interface or not, rather than try.
      case "scala.Option" => Try(doProxy(data, innerGenericClass(method))) match {
          case Failure(_) => Try(doConvert(
              data.toString,
              innerGenericClass(method)
            )) match {
              case Failure(_) => None
              case Success(value) => Some(value)
            }
          case Success(value) => Some(value)
        }
      case _ => Try(doProxy(data, method.getReturnType)) match {
          case Failure(_) => doConvert(data.toString, method.getReturnType)
          case Success(value) => value
        }
    }
  }
  private def handleArray[T](data: Iterator[JsonNode], target: Class[T]): List[T] = {
    data.toList.map {
      // TODO Array item of simple vs Array item of complex, same here, later check if target class is interface or not, rather than try
      case item: ObjectNode => Try(doProxy(item, target)) match {
          case Failure(_) => doConvert(
              item.toString,
              target
            )
          case Success(value) => value
        }
      case item => doConvert(
          item.toString,
          target
        )
    }
  }

  // Convert the JsonNode to something else
  private def doConvert[T](value: String, returning: Class[T]): T = {
    val pattern = "\\$\\{([^}]+)}".r
    val replaced = pattern.findAllIn(value).matchData.foldLeft(value) {
      (str, matchData) =>
        val target = matchData.group(1).trim
        str.replaceAllLiterally(
          matchData.matched,
          env.getProperty(target, System.getProperty(target, System.getenv(target)))
        )
    }
    objectMapper.readValue(replaced, returning)
  }

  // Proxy the JsonNode as an interface
  private def doProxy[T](data: JsonNode, target: Class[T]): T = {
    JacksonProxyConfigProvider(data, objectMapper, env).proxy(target)
  }

}

case class JacksonProxyConfigProvider(
    private val data: JsonNode,
    private val objectMapper: ObjectMapper,
    private val env: Properties)
    extends ConfigProvider with ConfigBuilder {

  override def withSources(configSources: ConfigSource*): JacksonProxyConfigProvider =
    JacksonProxyConfigProvider(
      // Merge new sources with existing JsonNode
      merge(configSources, data),
      objectMapper,
      env
    )

  override def withEnvs(configEnvs: EnvSource*): ConfigBuilder with ConfigProvider =
    JacksonProxyConfigProvider(
      // Merge new sources with existing JsonNode
      data,
      objectMapper,
      merge(configEnvs, env)
    )

  def chunk(path: String): JacksonProxyConfigProvider =
    JacksonProxyConfigProvider(data.findValue(path), objectMapper, env)

  private def proxy[T](clazz: Class[T], newData: JsonNode): T = {
    // We will return a proxy that will delegate method calls that are backed by a JsonNode and Jackson
    Proxy.newProxyInstance(
      // Use the classLoader of the target class?
      clazz.getClassLoader,
      // This anonymous proxy will only be implementing a single interface/trait (which is our target marshalling class)
      Array(clazz),
      // Uses Jackson to delegate method calls backed by a JsonNode
      JsonNodeBackedJacksonInvocationHandler(
        newData,
        objectMapper,
        env
      )
      // Our proxy will implement our target interface/trait (clazz), so we will force cast it to look like this type
    ).asInstanceOf[T]
  }

  def proxy[T](path: String, clazz: Class[T]): T = {
    // First extract the value at the json path from the root node, then use this as the proxy data
    proxy(clazz, data.findValue(path))
  }

  def proxy[T](clazz: Class[T]): T = {
    // Use the root data as the proxy data
    proxy(clazz, data)
  }
}
