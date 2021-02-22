package andy42.ssc.analysis

import andy42.ssc.config.Config
import andy42.ssc.{StreamProcessing, TweetExtract}
import cats.data.Reader
import cats.effect.{Blocker, ContextShift, IO, Sync}
import fs2.Stream
import io.circe.Json
import jawnfs2.JsonStreamSyntax
import org.typelevel.jawn.Facade

import java.nio.file.Paths
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext


object TweetConversionConcurrency extends App {

  implicit val executionContext: ExecutionContext = ExecutionContext.global
  implicit val contextShift: ContextShift[IO] = IO.contextShift(executionContext)

  val dataDirectory = "data"
  val captureFileName = "tweet-capture.txt"

  val executorService = Executors.newFixedThreadPool(2)

  def initialStream[F[_] : Sync : ContextShift]: Reader[Config, Stream[F, Json]] = Reader { _: Config =>

    implicit val f: Facade[Json] = new io.circe.jawn.CirceSupportParser(None, allowDuplicateKeys = false).facade

    val blocker: Blocker = Blocker.liftExecutionContext(ExecutionContext.fromExecutor(executorService))

    fs2.io.file
      .readAll[F](Paths.get(dataDirectory, captureFileName), blocker, 4096)
      .chunks.parseJsonStream
  }

  val streamProducer: Reader[Config, Stream[IO, TweetExtract]] = for {
    jsonStream <- initialStream[IO]
    tweetExtractStream <- StreamProcessing.decodeToTweetExtract(jsonStream)
  } yield tweetExtractStream

  def timeConversion(concurrency: Int): Long = {

    val config = Config.config.copy(
      streamParameters = Config.config.streamParameters.copy(extractConcurrency = concurrency))

    val start = System.currentTimeMillis()

    streamProducer
      .run(config)
      .compile
      .drain
      .unsafeRunSync()

    System.currentTimeMillis() - start
  }

  println("|Concurrency|Time (ms)|")
  println("|---:|---:|")
  (1 to 10).foreach(concurrency => println(s"|$concurrency|${timeConversion(concurrency)}|"))

  executorService.shutdown()
}
