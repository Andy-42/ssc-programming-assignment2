package andy42.ssc

import andy42.ssc.config.Config
import cats.FlatMap
import cats.effect.Clock
import com.codahale.metrics.Meter
import fs2.{Chunk, Pure, Stream}
import cats.syntax.functor._

import scala.concurrent.duration.MILLISECONDS


/** The current summary state.
  *
  * @param summariesByWindow The summaries being collected for each window.
  * @param rateMeter         A DropWizard rate meter for calculating tweet rates and total counts.
  *                          Note that we only count tweets that make it to this summary calculation stage,
  *                          and ignore other tweets that have been discarded at earlier stages of processing.
  */
case class WindowSummaries private(summariesByWindow: Map[WindowStart, WindowSummary],
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

  def configuredChunkedTweetCombiner[F[_] : FlatMap](clock: Clock[F], config: Config)
  : (WindowSummaries, Chunk[TweetExtract]) => F[(WindowSummaries, Stream[Pure, WindowSummaryOutput])] = {

    val eventTime = EventTime(config.eventTime)
    val windowSummaryOutput: (WindowSummary, Meter) => WindowSummaryOutput = WindowSummaryOutput(config)

    (windowSummaries: WindowSummaries, tweetExtracts: Chunk[TweetExtract]) =>

      for {
        now <- clock.realTime(MILLISECONDS)

        _ = windowSummaries.rateMeter.mark(tweetExtracts.size.toLong)

        // Update the window summaries for each distinct window start time, but only for non-expired windows.
        updatedSummaries = windowSummaries.summariesByWindow ++ (for {
          windowStart <- tweetExtracts.iterator.map(_.windowStart).distinct
          if !eventTime.isExpired(createdAt = windowStart, now = now)
          previousSummaryForWindow = windowSummaries.summariesByWindow.getOrElse(
            key = windowStart, default = WindowSummary(windowStart = windowStart, now = now))
        } yield windowStart -> previousSummaryForWindow.add(tweetExtracts, now))

        // Separate expired windows from windows for which collection is ongoing
        (expired, ongoing) = updatedSummaries.partition { case (windowStart, _) =>
          eventTime.isExpired(createdAt = windowStart, now = now)
        }

        // The next value for this object
        nextWindowSummaries = WindowSummaries(summariesByWindow = ongoing, rateMeter = windowSummaries.rateMeter)

        // Any summaries that are expired get reported
        output = expired.map { case (_, windowSummary) => windowSummaryOutput(windowSummary, windowSummaries.rateMeter) }
      } yield (nextWindowSummaries, Stream.emits(output.toSeq))
  }
}
