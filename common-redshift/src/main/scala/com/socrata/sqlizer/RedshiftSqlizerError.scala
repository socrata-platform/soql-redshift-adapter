package com.socrata.sqlizer

import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.util.AutomaticJsonCodec
import com.socrata.soql.environment.ScopedResourceName
import com.socrata.soql.util.{EncodedError, SoQLErrorCodec, SoQLErrorDecode, SoQLErrorEncode}

import scala.util.parsing.input.Position


sealed abstract class RedshiftSqlizerError[+RNS]

object RedshiftSqlizerError {
  case class NonLiteralContextParameter[+RNS](source: ScopedResourceName[RNS], position: Position) extends RedshiftSqlizerError[RNS]
  object NonLiteralContextParameter {
    private val tag = "soql.redshift.non-literal-context-parameter"

    // This isn't actually necessary for NonLiteralContextParameter,
    // but for an error with more than just "source" and "position",
    // this case class would have all the fields except those two.
    // Since this class has exactly those two, this is empty:
    @AutomaticJsonCodec
    private case class Fields()

    implicit def encode[RNS: JsonEncode] = new SoQLErrorEncode[NonLiteralContextParameter[RNS]] {
      override val code = tag

      def encode(err: NonLiteralContextParameter[RNS]) =
        result(
          Fields(), // Extract extra data from the error
          "Non-literal context parameter", // human-readable explanation of the error
          err.source,
          err.position
        )
    }

    implicit def decode[RNS: JsonDecode] = new SoQLErrorDecode[NonLiteralContextParameter[RNS]] {
      override val code = tag

      def decode(v: EncodedError) =
        for {
//          fields <- data[Fields](v)
          source <- source[RNS](v)
          position <- position(v)
        } yield {
          // if there were more fields, their values would be pulled
          // out of `fields`
          NonLiteralContextParameter(source, position)
        }
    }
  }


  def errorCodecs[RNS : JsonEncode : JsonDecode, T >: RedshiftSqlizerError[RNS] <: AnyRef](
                                                                                            codecs: SoQLErrorCodec.ErrorCodecs[T] = new SoQLErrorCodec.ErrorCodecs[T]
                                                                                          ): SoQLErrorCodec.ErrorCodecs[T] =
    codecs
      .branch[NonLiteralContextParameter[RNS]]
}
