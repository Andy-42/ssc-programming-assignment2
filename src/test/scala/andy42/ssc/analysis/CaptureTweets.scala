package andy42.ssc.analysis

import andy42.ssc.TWStream
import andy42.ssc.config.Config
import cats.effect.{Blocker, ContextShift, IO}
import fs2.io.file.writeAll
import fs2.text.utf8Encode
import io.circe.syntax._

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext


object CaptureTweets extends App {

  val executorService = Executors.newFixedThreadPool(2)
  val blocker: Blocker = Blocker.liftExecutionContext(ExecutionContext.fromExecutor(executorService))

  implicit val executionContext: ExecutionContext = ExecutionContext.global
  implicit val contextShift: ContextShift[IO] = IO.contextShift(executionContext)

  val dataDirectory = "data"
  val captureFileName = "tweet-capture.txt"
  new File(dataDirectory).mkdirs()
  Paths.get(dataDirectory, captureFileName).toFile.delete()

  TWStream.jsonStream[IO]
    .run(Config.config)
    .take(100000)

    // For demo purposes, print a JSON representation to the console as WindowSummaryOutput are emitted
    .map(_.asJson.noSpaces.toString)
    .intersperse(separator = "\n")
    .through(utf8Encode)

    .through(writeAll(Paths.get(dataDirectory, captureFileName), blocker))

    // Make it so
    .compile
    .drain
    .unsafeRunSync()

  executorService.shutdown()
}
