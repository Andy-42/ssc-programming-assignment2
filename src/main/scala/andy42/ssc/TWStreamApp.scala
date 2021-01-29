package andy42.ssc

import andy42.ssc.config.Config.StreamParametersConfig
import cats.effect._
import fs2.io.stdout
import fs2.text.utf8Encode
import fs2.{Chunk, Stream}
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
      .parEvalMapUnordered(StreamParametersConfig.extractConcurrency)(TweetExtract.decode)
      .flatMap(identity)


      // Aggregate tweet extracts in windows, and emit them as windows expire

      // This version of the aggregation consumes one TweetExtract at a time
      //      .evalScan((WindowSummaries(), Stream.emits(Seq.empty[WindowSummaryOutput]))) {
      //        case ((windowSummaries, _), tweetExtract: TweetExtract) =>
      //          WindowSummaries.combineTweet[IO](windowSummaries, tweetExtract)
      //      }
      //      .flatMap(_._2)

      // This version of the aggregation consumes a Chunk[TweetExtract] for efficiency.
      // The function groups into chunks based on the window that the tweet was created in,
      // which both works out nicely because tweets arrive in that order, but also the
      // aggregation logic in WindowSummaries.combineChunkedTweet requires that all tweets
      // in a chunk are in the same window (for simplicity of the logic).
      // TODO: Use `Stream.groupWithin` to bound the size and emit deadline for chunks
      // TODO: and refactor `WindowSummaries.combineChunkedTweet` to deal with mixed createTime chunks.
      .groupAdjacentByLimit(limit = StreamParametersConfig.chunkSizeLimit)(_.createdAt)
      .evalScan((WindowSummaries(), Stream.emits(Seq.empty[WindowSummaryOutput]))) {
        case ((windowSummaries, _), tweetExtractChunk: (Long, Chunk[TweetExtract])) =>
          WindowSummaries.combineChunkedTweet[IO](windowSummaries, tweetExtractChunk._2)
      }
      .flatMap(_._2)

      // For demo purposes, print a JSON representation to the console as they are emitted
      .map(_.asJson.spaces2 + "\n")
      .through(utf8Encode)
      .through(stdout(blocker))

      .compile
      .drain
      .as(ExitCode.Success)
  }
}
