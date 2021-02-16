package andy42.ssc

import andy42.ssc.config.Config
import cats.data.Reader
import cats.effect.{Clock, Concurrent, IO, Timer}
import fs2.{Chunk, Pure, Stream}
import io.circe.Json


object StreamProcessing {

  def decodeToTweetExtract[F](initialStream: Stream[IO, Json])
                             (implicit concurrent: Concurrent[IO]): Reader[Config, Stream[IO, TweetExtract]] =
    Reader { config: Config =>
      val decode: Json => IO[Stream[Pure, TweetExtract]] = TweetExtract.decode(EventTime(config.eventTime))
      val maxConcurrent = config.streamParameters.extractConcurrency

      // Map the incoming tweets in JSON format to extracts with only the aspects this stream monitors.
      // The presumption is that this stage will use significant CPU, so we can increase the concurrency
      // to use available cores to increase overall throughput.
      initialStream
        .parEvalMapUnordered(maxConcurrent)(decode)
        .flatten
    }

  def summarizeStream[F[_] : Clock : Timer : Concurrent](extractedTweetStream: Stream[F, TweetExtract])
  : Reader[Config, Stream[F, WindowSummaryOutput]] = Reader { config =>

    // Combines a chunk of tweets into a WindowSummaries.
    // This uses a Clock to determine when windows are expired.
    val summaryCombiner:
      (WindowSummaries, Chunk[TweetExtract]) => F[(WindowSummaries, Stream[Pure, WindowSummaryOutput])] =
      WindowSummaries.configuredChunkedTweetCombiner[F](Clock[F], config)

    // Group in chunks as large as possible, but put an upper limit on how long we will wait before emitting.
    extractedTweetStream
      .groupWithin(n = config.streamParameters.chunkSizeLimit, d = config.streamParameters.chunkGroupTimeout)

      // Aggregate tweet extracts in windows, and emit them as windows expire.
      .evalScan((WindowSummaries(), Stream.emits(Seq.empty[WindowSummaryOutput]))) {
        case ((windowSummaries, _), tweetExtractChunk) => summaryCombiner(windowSummaries, tweetExtractChunk)
      }
      .flatMap(_._2)
  }
}
