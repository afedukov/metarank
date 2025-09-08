package ai.metarank.model

import io.circe.{Codec, Decoder, DecodingFailure, Encoder}
import io.circe.generic.semiauto._

import java.util

sealed trait Field {
  def name: String
}

object Field {
  case class StringField(name: String, value: String)           extends Field
  case class BooleanField(name: String, value: Boolean)         extends Field
  case class NumberField(name: String, value: Double)           extends Field
  case class StringListField(name: String, value: List[String]) extends Field
  case class NumberListField(name: String, value: Array[Double]) extends Field {
    override def equals(obj: Any): Boolean = obj match {
      case NumberListField(xname, xvalues) => (name == xname) && (util.Arrays.equals(value, xvalues))
      case _                               => false
    }
  }

  case class ScalarField(name: String, value: Scalar) extends Field {
    override def equals(obj: Any): Boolean = obj match {
      case ScalarField(xname, xvalue) => name == xname && value == xvalue
      case _                          => false
    }
    override def hashCode(): Int = java.util.Objects.hash(name, value)
  }

  object NumberListField {}

  def toString(fields: List[Field]) = fields
    .map {
      case Field.StringField(name, value)     => s"$name=$value"
      case Field.BooleanField(name, value)    => s"$name=$value"
      case Field.NumberField(name, value)     => s"$name=$value"
      case Field.StringListField(name, value) => s"$name=${value.mkString(",")}"
      case Field.NumberListField(name, value) => s"$name=${value.mkString(",")}"
      case Field.ScalarField(name, value)     => s"$name=$value"
    }
    .mkString("[", ", ", "]")

  implicit val fieldDecoder: Decoder[Field] = Decoder.instance(c =>
    for {
      name <- c.downField("name").as[String]
      fieldJson <- c.downField("value").focus match {
        case Some(value) => Right(value)
        case None        => Left(DecodingFailure(s"field value not found", c.history))
      }
      field <- fieldJson.fold(
        jsonNull = Left(DecodingFailure(s"null value in field $name", c.history)),
        jsonBoolean = value => Right(BooleanField(name, value)),
        jsonNumber = value => Right(NumberField(name, value.toDouble)),
        jsonString = value => Right(StringField(name, value)),
        jsonArray = {
          case values if values.forall(_.isString) => Right(StringListField(name, values.flatMap(_.asString).toList))
          case values if values.forall(_.isNumber) =>
            Right(ScalarField(name, Scalar.SDoubleList(values.flatMap(_.asNumber.map(_.toDouble)).toArray)))
          case other =>
            Left(DecodingFailure(s"cannot decode field $name: got list of $other", c.history))
        },
        jsonObject = obj => Left(DecodingFailure(s"cannot decode field $name: got object $obj", c.history))
      )
    } yield {
      field
    }
  )

  implicit val stringEncoder: Encoder[StringField]         = deriveEncoder
  implicit val boolEncoder: Encoder[BooleanField]          = deriveEncoder
  implicit val numEncoder: Encoder[NumberField]            = deriveEncoder
  implicit val stringListEncoder: Encoder[StringListField] = deriveEncoder
  implicit val numListEncoder: Encoder[NumberListField]    = deriveEncoder
  implicit val scalarFieldEncoder: Encoder[ScalarField]    = deriveEncoder

  implicit val fieldEncoder: Encoder[Field] = Encoder.instance {
    case f: StringField     => stringEncoder.apply(f)
    case f: BooleanField    => boolEncoder.apply(f)
    case f: NumberField     => numEncoder.apply(f)
    case f: StringListField => stringListEncoder.apply(f)
    case f: NumberListField => numListEncoder.apply(f)
    case f: ScalarField     => scalarFieldEncoder.apply(f)
  }

  implicit val fieldCodec: Codec[Field] = Codec.from(fieldDecoder, fieldEncoder)
}
