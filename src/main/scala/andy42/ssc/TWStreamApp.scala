package andy42.ssc

import andy42.ssc.StreamProcessing._
import andy42.ssc.config.Config
import cats.data.Reader
import cats.effect._
import fs2.Stream
import fs2.io.stdout
import fs2.text.utf8Encode
import io.circe.generic.auto._
import io.circe.syntax._

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext


object TWStreamApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {

    val streamProducer: Reader[Config, Stream[IO, WindowSummaryOutput]] =
      for {
        // Start with the stream of Tweet JSON coming from the Twitter sample stream.
        initialStream <- TWStream.jsonStream[IO]

        // Decode tweet JSON to TweetExtract
        extractedTweetStream <- decodeToTweetExtract(initialStream)

        // Summarize the Stream of TweetExtract into tumbling windows, and emit summary output as windows expire
        summaryOutputStream <- summarizeStream(extractedTweetStream)
      } yield summaryOutputStream

    // Writing to stdout blocks, so it should use this separate
    // Blocker execution context to avoid blocking the general-purpose pool.
    // Note that this is not passed an implicit.
    val blocker: Blocker = Blocker.liftExecutionContext(
      ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
    )

    streamProducer
      .run(Config.config)

      // For demo purposes, print a JSON representation to the console as WindowSummaryOutput are emitted
      .map(_.asJson.spaces2)
      .intersperse(separator = "\n")
      .through(utf8Encode)
      .through(stdout(blocker))

      // Make it so
      .compile
      .drain
      .as(ExitCode.Success)
  }
}
