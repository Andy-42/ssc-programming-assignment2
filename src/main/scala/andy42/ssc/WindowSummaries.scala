package andy42.ssc

import cats.effect.{Clock, Sync}
import cats.syntax.functor._
import com.codahale.metrics.Meter
import fs2.{Chunk, Pure, Stream}

import scala.concurrent.duration.MILLISECONDS


/** The current summary state.
  *
  * @param summariesByWindow The summaries being collected for each window.
  *                          The key is the created_at time (adjusted to the start of the window).
  * @param rateMeter         A DropWizard rate meter for calculating tweet rates and total counts.
  *                          Note that we only count tweets that make it to this summary calculation stage,
  *                          and ignore other tweets that have been discarded at earlier stages of processing.
  */
case class WindowSummaries private(summariesByWindow: Map[Long, WindowSummary],
                                   rateMeter: Meter)

/** Provides methods for aggregating tweet extracts into WindowSummaries state.
  *
  * Accessing the system clock is effectful, these methods return a result suspended in `F[_]`.
  * While the system clock is modeled accurately, the DropWizard Meter (used for rate calculations) is not
  * modeled correctly since it also uses the system clock internally. This is a punt for the purposes of this
  * exercise.
  */
object WindowSummaries {

  def apply(): WindowSummaries = WindowSummaries(summariesByWindow = Map.empty, rateMeter = new Meter())


  /** Combines a Chunk[TweetExtract] with an existing set of window summaries.
    * Produces a tuple containing the next state and any output,
    * which is intended to be used within a scan of a TweetExtract stream,
    * producing a Stream of WindowSummaryOutput.
    *
    * This implementation requires that all the elements of the Chunk[TweetExtract]
    * fall within the same window. This simplifies the processing here, and is likely to
    * work well assuming that the stream of tweets are delivered in approximately real time.
    * See: `Stream.groupAdjacentByLimit`
    * That assumption remains to be tested!
    *
    * @param windowSummaries an existing WindowSummaries.
    * @param tweetExtracts   A Chunk[TweetExtract].
    * @return A tuple containing the next state for window summaries, and an iterable containing
    *         the output summary for any windows that expired.
    */
  def combineChunkedTweet[F[_] : Sync : Clock](windowSummaries: WindowSummaries, tweetExtracts: Chunk[TweetExtract])
  : F[(WindowSummaries, Stream[Pure, WindowSummaryOutput])] =
    for {
      now <- Clock[F].realTime(MILLISECONDS)

      _ = windowSummaries.rateMeter.mark(tweetExtracts.size.toLong)

      // Update the window summaries for each distinct window start time, but only for non-expired windows.
      updatedSummaries = windowSummaries.summariesByWindow ++ (for {
        windowStart <- tweetExtracts.foldLeft(Set.empty[Long])(_ + _.createdAt)
        if !EventTime.isExpired(createdAt = windowStart, now = now)
        previousSummaryForWindow = windowSummaries.summariesByWindow.getOrElse(
          key = windowStart, default = WindowSummary(windowStart = windowStart, now = now))
      } yield windowStart -> previousSummaryForWindow.add(tweetExtracts, now))

      // Separate expired windows from windows for which collection is ongoing
      (expired, ongoing) = updatedSummaries.partition { case (createdAt, _) =>
        EventTime.isExpired(createdAt = createdAt, now = now)
      }

      // The next value for this object
      nextWindowSummaries = WindowSummaries(summariesByWindow = ongoing, rateMeter = windowSummaries.rateMeter)

      // Any summaries that are expired get reported
      output = expired.map { case (_, windowSummary) => WindowSummaryOutput(windowSummary, windowSummaries.rateMeter) }
    } yield (nextWindowSummaries, Stream.emits(output.toSeq))
}
