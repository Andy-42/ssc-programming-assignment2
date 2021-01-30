package andy42.ssc

import andy42.ssc.config.Config.{StreamParametersConfig => Config}
import cats.effect._
import fs2.{Pure, Stream}
import fs2.io.stdout
import fs2.text.utf8Encode
import io.circe.generic.auto._
import io.circe.syntax._


object TWStreamApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {

    implicit val blocker: Blocker = Blocker.liftExecutionContext(scala.concurrent.ExecutionContext.global)

    // Required by WindowsSummaries.combineTweet/combineChunkedTweet
    implicit val clock: Clock[IO] = Clock.create[IO]

    // Start with the stream of Tweet JSON coming from the Twitter sample stream.
    new TWStream[IO].jsonStream()

      // Map the incoming tweets in JSON format to extracts with only the aspects this stream monitors.
      // The presumption is that this stage will use significant CPU, so we can increase the concurrency
      // to use available core to increase overall throughput.
      .parEvalMapUnordered(Config.extractConcurrency)(TweetExtract.decode)
      .flatMap(identity)

      // Group in chunks as large as possible, but put an upper limit on how long we will wait before emitting.
      .groupWithin(n = Config.chunkSizeLimit, d = Config.chunkGroupTimeout)

      // Aggregate tweet extracts in windows, and emit them as windows expire.
      .evalScan((WindowSummaries(), Stream.emits(Seq.empty[WindowSummaryOutput]))) {
        case ((windowSummaries, _), tweetExtractChunk) =>
          WindowSummaries.combineChunkedTweet[IO](windowSummaries, tweetExtractChunk)
      }
      .flatMap { case (_, output: Stream[Pure, WindowSummaryOutput]) => output }

      // For demo purposes, print a JSON representation to the console as they are emitted
      .map(_.asJson.spaces2 + "\n")
      .through(utf8Encode)
      .through(stdout(blocker))

      // Make it so
      .compile
      .drain
      .as(ExitCode.Success)
  }
}
