package andy42.ssc

import fs2.Chunk
import fs2.Stream
import fs2.Pure
import com.codahale.metrics.Meter
import cats.effect.{Clock, Sync}
import cats.syntax.functor._

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

  /** Combines a tweet with an existing set of window summaries.
    * Produces a tuple containing the next state and any output,
    * which is intended to be used within a scan of a TweetExtract stream,
    * producing a Stream of WindowSummaryOutput.
    *
    * @param windowSummaries A existing WindowSummaries.
    * @param tweetExtract    A single tweet extract.
    * @return A tuple containing the next WindowSummaries, and an iterable containing
    *         the output summary for any windows that expired.
    */
  def combineTweet[F[_] : Sync : Clock](windowSummaries: WindowSummaries, tweetExtract: TweetExtract)
  : F[(WindowSummaries, Stream[Pure, WindowSummaryOutput])] =
    for {
      now <- Clock[F].realTime(MILLISECONDS)

      _ = windowSummaries.rateMeter.mark() // effectful

      // The new summaries by window that includes the counts from this tweet
      updatedSummariesByWindow =
      if (EventTime.isExpired(createdAt = tweetExtract.createdAt, now = now))
        windowSummaries.summariesByWindow
      else {
        val updatedSummaryForWindow = windowSummaries.summariesByWindow
          .get(tweetExtract.createdAt)
          .fold(WindowSummary(tweetExtract)) { windowSummary: WindowSummary => windowSummary.add(tweetExtract) }

        windowSummaries.summariesByWindow + (tweetExtract.createdAt -> updatedSummaryForWindow)
      }

      // Separate expired windows from windows for which collection is ongoing
      (expired, ongoing) = updatedSummariesByWindow.partition { case (createdAt, _) =>
        EventTime.isExpired(createdAt = createdAt, now = now)
      }

      // The next value for this object
      nextWindowSummaries = WindowSummaries(summariesByWindow = ongoing, rateMeter = windowSummaries.rateMeter)

      // Any summaries that are expired get reported
      output = expired.map { case (_, windowSummary) => WindowSummaryOutput(windowSummary, windowSummaries.rateMeter) }
    } yield (nextWindowSummaries, Stream.emits(output.toSeq))

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

      // The new summaries by window that includes the counts from this chunk of tweets
      updatedSummariesByWindow: Map[Long, WindowSummary] =
      if (EventTime.isExpired(createdAt = now, now = now))
        windowSummaries.summariesByWindow
      else {
        val updatedSummaryForWindow = windowSummaries.summariesByWindow
          .get(now)
          .fold(WindowSummary(tweetExtracts)) { windowSummary: WindowSummary => windowSummary.add(tweetExtracts) }

        windowSummaries.summariesByWindow + (now -> updatedSummaryForWindow)
      }

      // Separate expired windows from windows for which collection is ongoing
      (expired: Map[Long, WindowSummary], ongoing) = updatedSummariesByWindow.partition { case (createdAt, _) =>
        EventTime.isExpired(createdAt = createdAt, now = now)
      }

      // The next value for this object
      nextWindowSummaries = WindowSummaries(summariesByWindow = ongoing, rateMeter = windowSummaries.rateMeter)

      // Any summaries that are expired get reported
      output = expired.map { case (_, windowSummary) => WindowSummaryOutput(windowSummary, windowSummaries.rateMeter) }
    } yield (nextWindowSummaries, Stream.emits(output.toSeq))
}
