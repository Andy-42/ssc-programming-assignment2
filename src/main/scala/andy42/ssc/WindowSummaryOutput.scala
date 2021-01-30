package andy42.ssc

import andy42.ssc.config.Config.{SummaryOutputConfig => Config}
import com.codahale.metrics.Meter

import java.time.Instant


/** The summary of tweets in a time window.
  *
  * @param windowStart               The window start as an ISO8601 string.
  * @param windowEnd                 The window end as an ISO8601 string.
  * @param windowLastUpdate          The instant when the last update to this window was applied.
  * @param totalTweetCount           The total number of tweets seen in the scan, including tweets not counted in any
  *                                  window.
  * @param tweetCountThisWindow      The total number of tweets that were counted in this window.
  * @param oneMinuteRate             The one minute rate. This is unrelated to the window, but sampled at the point
  *                                  where this summary was emitted.
  * @param fiveMinuteRate            The five minute rate. This is unrelated to the window, but sampled at the point
  *                                  where this summary was emitted.
  * @param fifteenMinuteRate         The fifteen minute rate. This is unrelated to the window, but sampled at the point
  *                                  where this summary was emitted.
  * @param topEmojis                 The top N emojis occurring within this window.
  * @param topHashtags               The top N hashtags occurring within this window.
  * @param topDomains                The top N domains occurring within this windows.
  * @param tweetsWithEmojiPercent    The percentage of tweets in this window that contain at least one emoji.
  * @param tweetsWithUrlPercent      The percentage of tweets in this window that contain at least one URL.
  * @param tweetsWithPhotoUrlPercent The percentage of tweets in this window that contain at least one photo URL.
  */
case class WindowSummaryOutput(windowStart: String,
                               windowEnd: String,
                               windowLastUpdate: String,

                               totalTweetCount: Long,

                               tweetCountThisWindow: Long,

                               oneMinuteRate: Double,
                               fiveMinuteRate: Double,
                               fifteenMinuteRate: Double,

                               topEmojis: List[String],
                               topHashtags: List[String],
                               topDomains: List[String],

                               tweetsWithEmojiPercent: Double,
                               tweetsWithUrlPercent: Double,
                               tweetsWithPhotoUrlPercent: Double
                              )

object WindowSummaryOutput {

  def apply(windowSummary: WindowSummary,
            rateMeter: Meter): WindowSummaryOutput = {

    WindowSummaryOutput(
      windowStart = Instant.ofEpochMilli(windowSummary.createdAt).toString,
      windowEnd = Instant.ofEpochMilli(EventTime.toWindowEnd(windowSummary.createdAt)).toString,
      windowLastUpdate = Instant.ofEpochMilli(windowSummary.lastWindowUpdate).toString,

      totalTweetCount = rateMeter.getCount,

      tweetCountThisWindow = windowSummary.tweets,

      oneMinuteRate = rateMeter.getOneMinuteRate,
      fiveMinuteRate = rateMeter.getFiveMinuteRate,
      fifteenMinuteRate = rateMeter.getFifteenMinuteRate,

      topEmojis = top(windowSummary.emojiCounts).toList,
      topDomains = top(windowSummary.domainCounts).toList,
      topHashtags = top(windowSummary.hashtagCounts).toList,

      tweetsWithEmojiPercent = 100.0 * windowSummary.tweetsWithEmoji / windowSummary.tweets,
      tweetsWithUrlPercent = 100.0 * windowSummary.tweetsWithUrl / windowSummary.tweets,
      tweetsWithPhotoUrlPercent = 100.0 * windowSummary.tweetsWithUrl / windowSummary.tweets
    )
  }

  /** Get the top N values by counts in descending order. */
  def top(counts: Map[String, Long]): Seq[String] =
    counts.toSeq
      .sortBy { case (_, count) => -count } // descending
      .take(Config.topN)
      .map { case (key, _) => key } // Keep the keys only
}
