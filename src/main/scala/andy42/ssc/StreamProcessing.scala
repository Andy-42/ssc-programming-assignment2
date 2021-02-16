package andy42.ssc

import andy42.ssc.config.Config
import cats.data.Reader
import cats.effect.{Clock, Concurrent, Timer}
import fs2.{Chunk, Pure, Stream}
import io.circe.Json


object StreamProcessing {

  def decodeToTweetExtract[F[_] : Concurrent](initialStream: Stream[F, Json]): Reader[Config, Stream[F, TweetExtract]] =
    Reader { config: Config =>
      val decode: Json => Stream[Pure, TweetExtract] = TweetExtract.decode(EventTime(config.eventTime))

      //      val maxConcurrent = config.streamParameters.extractConcurrency

      //      // Map the incoming tweets in JSON format to extracts with only the aspects this stream monitors.
      //      // The presumption is that this stage will use significant CPU, so we can increase the concurrency
      //      // to use available cores to increase overall throughput.
      //      initialStream
      //        .parEvalMapUnordered(maxConcurrent = maxConcurrent)(f = (decode))
      //        .flatten

      initialStream.flatMap(decode)
    }

  def summarizeStream[F[_] : Clock : Timer : Concurrent](extractedTweetStream: Stream[F, TweetExtract])
  : Reader[Config, Stream[F, WindowSummaryOutput]] = Reader { config =>

    // Combines a chunk of tweets into a WindowSummaries.
    // This uses a Clock to determine when windows are expired.
    val summaryCombiner:
      (WindowSummaries, Chunk[TweetExtract]) => F[(WindowSummaries, Stream[Pure, WindowSummaryOutput])] =
      WindowSummaries.configuredChunkedTweetCombiner[F](Clock[F], config)

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
}
