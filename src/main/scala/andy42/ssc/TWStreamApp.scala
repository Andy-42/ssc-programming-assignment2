package andy42.ssc

import andy42.ssc.config.Config
import cats.data.Reader
import cats.effect._
import fs2.io.stdout
import fs2.text.utf8Encode
import fs2.{Pure, Stream}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext


object TWStreamApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {

    val streamProducer: Reader[Config, Stream[IO, WindowSummaryOutput]] = for {

      // Start with the stream of Tweet JSON coming from the Twitter sample stream.
      // TODO: Perhaps this stream should be supplied as a parameter so we can test with canned streams
      initialStream: Stream[IO, Json] <- TWStream.jsonStream[IO]

      extractedTweetStream <- Reader[Config, Stream[IO, TweetExtract]] { config =>

        val decode = TweetExtract.decode(EventTime(config.eventTime)) _

        // Map the incoming tweets in JSON format to extracts with only the aspects this stream monitors.
        // The presumption is that this stage will use significant CPU, so we can increase the concurrency
        // to use available cores to increase overall throughput.
        initialStream.parEvalMapUnordered(
          maxConcurrent = config.streamParameters.extractConcurrency)(decode)
          .flatMap(identity)
      }

      summaryOutputStream: Stream[IO, WindowSummaryOutput] <- Reader[Config, Stream[IO, WindowSummaryOutput]] { config =>

        // Combines a chunk of tweets into a WindowSummaries.
        // This uses a Clock to determine when windows are expired.
        val summaryCombiner = WindowSummaries.configuredChunkedTweetCombiner[IO](Clock[IO], config)

        // Group in chunks as large as possible, but put an upper limit on how long we will wait before emitting.
        extractedTweetStream.groupWithin(
          n = config.streamParameters.chunkSizeLimit,
          d = config.streamParameters.chunkGroupTimeout)

          // Aggregate tweet extracts in windows, and emit them as windows expire.
          .evalScan((WindowSummaries(), Stream.emits(Seq.empty[WindowSummaryOutput]))) {
            case ((windowSummaries, _), tweetExtractChunk) => summaryCombiner(windowSummaries, tweetExtractChunk)
          }
          .flatMap { case (_, output: Stream[Pure, WindowSummaryOutput]) => output }
      }
    } yield summaryOutputStream

    // Writing to stdout blocks, so it should use this separate
    // Blocker execution context to avoid blocking the general-purpose pool.
    // Note that this is not passed an implicit.
    val blocker: Blocker = Blocker.liftExecutionContext(
      ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
    )

    streamProducer(Config.config)
      // For demo purposes, print a JSON representation to the console as WindowSummaryOutput are emitted
      .map(_.asJson.spaces2)
      .intersperse("\n")
      .through(utf8Encode)
      .through(stdout(blocker))

      // Make it so
      .compile
      .drain
      .as(ExitCode.Success)
  }
}
