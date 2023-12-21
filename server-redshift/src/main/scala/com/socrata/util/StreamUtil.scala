package com.socrata.util

import org.apache.commons.io.IOUtils

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets

object StreamUtil {

  def normalize(input: InputStream): ByteArrayInputStream = {
    // TODO Why do we do this?
    new ByteArrayInputStream(IOUtils.toString(input, StandardCharsets.UTF_8.name).getBytes(StandardCharsets.ISO_8859_1))
  }

}
