package com.socrata.converter

import com.socrata.config.JacksonConfig
import jakarta.json.JsonObject
import org.eclipse.microprofile.config.spi.Converter

class JsonConverter extends Converter[JsonObject]{
  override def convert(value: String): JsonObject = {
    JacksonConfig.Default.readValue(value,classOf[JsonObject])
  }
}
