package com.socrata.common.sqlizer

import com.rojoma.json.v3.codec.{JsonDecode, JsonEncode}
import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.soql.environment.Source
import com.socrata.soql.util.{
  EncodedError,
  SoQLErrorCodec,
  SoQLErrorDecode,
  SoQLErrorEncode
}

sealed abstract class RedshiftSqlizerError[+RNS]

object RedshiftSqlizerError {
  case class NonLiteralContextParameter[+RNS](source: Source[RNS])
      extends RedshiftSqlizerError[RNS]

  object NonLiteralContextParameter {
    private val tag = "soql.redshift.non-literal-context-parameter"

    case class Fields()
    object Fields {
      implicit val jCodec = AutomaticJsonCodecBuilder[Fields]
    }

    implicit def encode[RNS: JsonEncode] =
      new SoQLErrorEncode[NonLiteralContextParameter[RNS]] {
        override val code = tag

        def encode(err: NonLiteralContextParameter[RNS]) =
          result(
            Fields(), // Extract extra data from the error
            "Non-literal context parameter", // human-readable explanation of the error
            err.source
          )
      }

    implicit def decode[RNS: JsonDecode] =
      new SoQLErrorDecode[NonLiteralContextParameter[RNS]] {
        override val code = tag

        def decode(v: EncodedError) =
          for {
            _ <- data[Fields](v)
            source <- source[RNS](v)
          } yield {
            NonLiteralContextParameter(source)
          }
      }
  }

  def errorCodecs[RNS: JsonEncode: JsonDecode, T >: RedshiftSqlizerError[
    RNS
  ] <: AnyRef](
      codecs: SoQLErrorCodec.ErrorCodecs[T] = new SoQLErrorCodec.ErrorCodecs[T]
  ) = {
    codecs
      .branch[NonLiteralContextParameter[RNS]]
      .build
  }
}
