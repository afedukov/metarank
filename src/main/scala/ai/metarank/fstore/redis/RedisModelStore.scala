package ai.metarank.fstore.redis

import ai.metarank.config.ModelConfig
import ai.metarank.fstore.Persistence.{ModelName, ModelStore}
import ai.metarank.fstore.codec.{KCodec, VCodec}
import ai.metarank.fstore.redis.client.RedisClient
import ai.metarank.ml.{Context, Model, Predictor}
import ai.metarank.util.Logging
import cats.effect.IO
import scala.concurrent.duration._

case class RedisModelStore(client: RedisClient, prefix: String)(implicit kc: KCodec[ModelName], vc: VCodec[Array[Byte]])
    extends ModelStore
    with Logging {
  override def put(value: Model[_]): IO[Unit] = for {
    bytesOption <- IO(value.save())
    _           <- info(s"serialized model ${value.name}, size=${bytesOption.map(_.length)}")
    _ <- bytesOption match {
      case None => info(s"model ${value.name} returned no payload, skipping redis write")
      case Some(bytes) =>
        val key     = kc.encode(prefix, ModelName(value.name))
        val payload = vc.encode(bytes)
        for {
          _ <- info(s"writing model ${value.name} to redis key=$key payloadSize=${payload.length}")
          _ <- client
            .set(key, payload, 9999.days)
            .handleErrorWith(err =>
              error(s"failed to persist model ${value.name} into redis key=$key: ${err.getMessage}", err).flatMap(_ =>
                IO.raiseError[Unit](err)
              )
            )
          _ <- info(s"model ${value.name} persisted into redis key=$key")
        } yield ()
    }
  } yield {}

  override def get[C <: ModelConfig, T <: Context, M <: Model[T]](
      key: ModelName,
      pred: Predictor[C, T, M]
  ): IO[Option[M]] = for {
    bytesOption <- client.get(kc.encode(prefix, key))
    model <- bytesOption match {
      case None =>
        pred.load(None).map(Some.apply)
      case Some(bytes) =>
        vc.decode(bytes) match {
          case Left(err)           => IO.raiseError(err)
          case Right(decodedBytes) => pred.load(Some(decodedBytes)).map(Some.apply)
        }
    }
  } yield {
    model
  }
}
