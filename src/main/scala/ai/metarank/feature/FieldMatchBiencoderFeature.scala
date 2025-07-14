package ai.metarank.feature

import ai.metarank.feature.BaseFeature.ItemFeature
import ai.metarank.feature.FieldMatchBiencoderFeature.FieldMatchBiencoderSchema
import ai.metarank.fstore.Persistence
import ai.metarank.ml.onnx.{EmbeddingCache, Normalize}
import ai.metarank.ml.onnx.ModelHandle.{HuggingFaceHandle, LocalModelHandle}
import ai.metarank.ml.onnx.Normalize.NoopNormalize
import ai.metarank.ml.onnx.distance.DistanceFunction
import ai.metarank.ml.onnx.distance.DistanceFunction.CosineDistance
import ai.metarank.ml.onnx.encoder.EncoderConfig
import ai.metarank.ml.onnx.encoder.EncoderConfig.BiEncoderConfig
import ai.metarank.ml.onnx.sbert.{OnnxBiEncoder, OnnxSession}
import ai.metarank.model.Dimension.SingleDim
import ai.metarank.model.Event.ItemEvent
import ai.metarank.model.Feature.ScalarFeature.ScalarConfig
import ai.metarank.model.FeatureValue.ScalarValue
import ai.metarank.model.Field.{StringField, StringListField}
import ai.metarank.model.Scalar
import ai.metarank.model.FieldName.EventType.{Item, Ranking}
import ai.metarank.model.{Event, Feature, FeatureSchema, FeatureValue, Field, FieldName, Key, MValue, ScopeType, Write}
import ai.metarank.model.Key.FeatureName
import ai.metarank.model.MValue.SingleValue
import ai.metarank.model.Scalar.SDoubleList
import ai.metarank.model.Scope.ItemScope
import ai.metarank.model.ScopeType.ItemScopeType
import ai.metarank.model.Write.Put
import ai.metarank.util.Logging
import cats.effect.{IO, Ref}
import io.circe.{Decoder, DecodingFailure}

import java.io.File
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

case class FieldMatchBiencoderFeature(
    schema: FieldMatchBiencoderSchema,
    encoder: Option[OnnxBiEncoder],
    itemCache: EmbeddingCache,
    rankingCache: EmbeddingCache
) extends ItemFeature
    with Logging {
  override def dim = SingleDim

  private val conf = ScalarConfig(
    scope = schema.scope,
    name = schema.name,
    refresh = schema.refresh.getOrElse(0.seconds),
    ttl = schema.ttl.getOrElse(90.days)
  )

  override def states: List[Feature.FeatureConfig] = List(conf)

  override def valueKeys(event: Event.RankingEvent): Iterable[Key] =
    event.items.toList.map(item => Key(ItemScope(item.id), conf.name))

  override def writes(event: Event, store: Persistence): IO[Iterable[Write]] = IO {
    event match {
      case e: ItemEvent =>
        for {
          field   <- e.fieldsMap.get(schema.itemField.field)
          encoded <- if (schema.preencoded) {
            field match {
              case Field.ScalarField(_, SDoubleList(vec)) => Some(vec.map(_.toFloat))
              case _                                       => None
            }
          } else {
            val strOpt = field match {
              case StringField(_, value)     => Some(value)
              case StringListField(_, value) => Some(value.mkString(" "))
              case _                         => None
            }
            strOpt.flatMap(str =>
              itemCache.get(e.item.value).orElse(encoder.flatMap(_.embed(Array(str)).headOption))
            )
          }
        } yield Put(Key(ItemScope(e.item), conf.name), e.timestamp, SDoubleList(encoded))
      case _ => None
    }
  }

  override def value(request: Event.RankingEvent, features: Map[Key, FeatureValue], id: Event.RankItem): MValue = ???

  override def values(
      request: Event.RankingEvent,
      features: Map[Key, FeatureValue],
      mode: BaseFeature.ValueMode
  ): List[MValue] = {
    val queryEmbeddingOption = request.fieldsMap.get(schema.rankingField.field) match {
      case Some(f) if schema.preencoded =>
        f match {
          case Field.ScalarField(_, SDoubleList(vec)) => Some(vec.map(_.toFloat))
          case _                                      => None
        }
      case Some(StringField(_, value)) if !schema.preencoded =>
        rankingCache.get(value).orElse(encoder.flatMap(_.embed(Array(value)).headOption))
      case Some(StringListField(_, value)) if !schema.preencoded =>
        val txt = value.mkString(" ")
        rankingCache.get(txt).orElse(encoder.flatMap(_.embed(Array(txt)).headOption))
      case _ => None
    }
    queryEmbeddingOption match {
      case None => request.items.toList.map(_ => SingleValue.missing(schema.name))
      case Some(queryEmbedding) =>
        val raw = request.items.toList.map(item => {
          features.get(Key(ItemScope(item.id), conf.name)) match {
            case Some(ScalarValue(_, ts, SDoubleList(emb), _)) =>
              MValue(schema.name.value, schema.distance.dist(queryEmbedding, emb.map(_.toFloat)))
            case _ => SingleValue.missing(schema.name)
          }
        })
        schema.norm.scale(raw)
    }
  }
}

object FieldMatchBiencoderFeature extends Logging {
  import ai.metarank.util.DurationJson._
  case class FieldMatchBiencoderSchema(
      name: FeatureName,
      rankingField: FieldName,
      itemField: FieldName,
      method: BiEncoderConfig,
      distance: DistanceFunction,
      norm: Normalize = NoopNormalize,
      refresh: Option[FiniteDuration] = None,
      ttl: Option[FiniteDuration] = None,
      preencoded: Boolean = false
  ) extends FeatureSchema {
    lazy val scope: ScopeType = ItemScopeType

    override def create(): IO[BaseFeature] =
      for {
        session <- method.model match {
          case Some(handle) =>
            OnnxSession.load(handle, method.dim, method.modelFile, method.tokenizerFile).map(Option.apply)
          case None => IO.none
        }
        items <- method.itemFieldCache match {
          case Some(path) if !(new File(path).exists()) =>
            info(s"cache file $path missing, ignoring") *> IO.pure(EmbeddingCache.empty())
          case Some(path) => EmbeddingCache.fromCSV(path, ',', method.dim)
          case None       => IO.pure(EmbeddingCache.empty())
        }
        fields <- method.rankingFieldCache match {
          case Some(path) if !(new File(path).exists()) =>
            info(s"cache file $path missing, ignoring") *> IO.pure(EmbeddingCache.empty())
          case Some(path) => EmbeddingCache.fromCSV(path, ',', method.dim)
          case None       => IO.pure(EmbeddingCache.empty())
        }
      } yield {
        FieldMatchBiencoderFeature(this, session.map(OnnxBiEncoder.apply), items, fields)
      }
  }

  object FieldMatchBiencoderSchema {
    implicit val biencSchemaDecoder: Decoder[FieldMatchBiencoderSchema] = Decoder.instance(c =>
      for {
        name <- c.downField("name").as[FeatureName]
        rankingField <- c.downField("rankingField").as[FieldName].flatMap {
          case ok @ FieldName(Ranking, _) => Right(ok)
          case other                      => Left(DecodingFailure(s"expected ranking field, but got $other", c.history))
        }
        itemField <- c.downField("itemField").as[FieldName].flatMap {
          case ok @ FieldName(Item, _) => Right(ok)
          case other                   => Left(DecodingFailure(s"expected item field, but got $other", c.history))
        }
        method     <- c.downField("method").as[BiEncoderConfig]
        distance   <- c.downField("distance").as[Option[DistanceFunction]]
        refresh    <- c.downField("refresh").as[Option[FiniteDuration]]
        ttl        <- c.downField("ttl").as[Option[FiniteDuration]]
        norm       <- c.downField("norm").as[Option[Normalize]]
        preencoded <- c.downField("preencoded").as[Option[Boolean]]
      } yield {
        FieldMatchBiencoderSchema(
          name = name,
          rankingField = rankingField,
          itemField = itemField,
          method = method,
          distance = distance.getOrElse(CosineDistance),
          refresh = refresh,
          ttl = ttl,
          norm = norm.getOrElse(NoopNormalize),
          preencoded = preencoded.getOrElse(false)
        )
      }
    )
  }

}
