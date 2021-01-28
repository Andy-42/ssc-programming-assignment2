package andy42.ssc

import cats.effect._
import com.typesafe.config.{Config, ConfigFactory}
import fs2.{Chunk, Stream}
import fs2.io.stdout
import fs2.text.utf8Encode
import io.circe.syntax._
import io.circe.generic.auto._


object TWStreamApp extends IOApp {

  val config: Config = ConfigFactory.load("application.conf").getConfig("stream-parameters")
  val ChunkSizeLimit: Int = config.getInt("chunk-size-limit")
  val ExtractConcurrency: Int = config.getInt("extract-concurrency")

  def run(args: List[String]): IO[ExitCode] = {

    implicit val blocker: Blocker = Blocker.liftExecutionContext(scala.concurrent.ExecutionContext.global)

    // Required by WindowsSummaries.combineTweet/combineChunkedTweet
    implicit val clock: Clock[IO] = Clock.create[IO]

    new TWStream[IO].jsonStream()

      // Map the incoming tweets in JSON format to extracts with only the aspects this stream monitors
      // TODO: Implement concurrent processing, assuming that the extract process uses significant CPU
      .flatMap(j => TweetExtract.decode(j))

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
      // TODO: Will the chunk size cause tweets to be delayed (and perhaps expire before aggregation)?
      .groupAdjacentByLimit(limit = ChunkSizeLimit)(_.createdAt)
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
