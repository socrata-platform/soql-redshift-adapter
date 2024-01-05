package com.socrata.common.config

import java.io.FileInputStream
import java.nio.file.Paths
import java.util.Properties
import scala.util.Using

case class PropertiesFileEnvSource(path: String) extends EnvSource {
  override def read(): Properties = {
    val properties = new Properties()
    val target = Paths.get(path).toAbsolutePath.toString
    Using(new FileInputStream(target)) { fileInputStream =>
      properties.load(fileInputStream)
    }
    properties
  }
}
