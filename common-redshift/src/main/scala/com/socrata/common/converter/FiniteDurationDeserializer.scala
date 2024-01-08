package com.socrata.common.converter

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.deser.std.StdDeserializer

import scala.concurrent.duration.{Duration, FiniteDuration}

class FiniteDurationDeserializer
    extends StdDeserializer[FiniteDuration](classOf[FiniteDuration]) {
  override def deserialize(
      p: JsonParser,
      ctxt: DeserializationContext
  ): FiniteDuration = {
    Duration(p.getText.trim) match {
      case _: Duration.Infinite =>
        throw ctxt.instantiationException(
          classOf[FiniteDuration],
          "Expected a finite duration, got infinite."
        )
      case duration: FiniteDuration => duration
    }
  }
}
