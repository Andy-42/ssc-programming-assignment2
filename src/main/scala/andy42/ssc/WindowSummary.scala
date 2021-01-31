package andy42.ssc

import fs2.Chunk


/** The summary of tweets within a given window.
  *
  * @param windowStart        The time that the tweet was created, adjusted to the start of the window
  *                           that the original created_at falls into.
  * @param lastWindowUpdate   The last time that an update was applied to this window summary.
  * @param tweets             The count of the number of tweets in this window.
  * @param tweetsWithEmoji    The count of tweets in this window that contain at least one emoji in the text.
  * @param tweetsWithUrl      The count of tweets in this window that contain at least one URL in the text.
  * @param tweetsWithPhotoUrl The count of tweets in this window that contain at least one photo URL in the text.
  * @param hashtagCounts      The count of the number of times each hashtag occurs in tweets in this window.
  * @param domainCounts       The count of the number of times each domain occurs in tweets in this window.
  * @param emojiCounts        The count of the number of times each emoji (or emoji sequence) occurs in this window.
  */
case class WindowSummary(windowStart: WindowStart,
                         lastWindowUpdate: EpochMillis,
                         tweets: Count,
                         tweetsWithEmoji: Count,
                         tweetsWithUrl: Count,
                         tweetsWithPhotoUrl: Count,
                         hashtagCounts: Map[String, Count],
                         domainCounts: Map[String, Count],
                         emojiCounts: Map[String, Count]) {

  import WindowSummary.addCounts

  /** Add the occurrences from a Chunk[TweetExtract] to a existing WindowSummary */
  def add(tweetExtracts: Chunk[TweetExtract], now: EpochMillis): WindowSummary = {

    def tweetsInThisWindow: Iterator[TweetExtract] = tweetExtracts.iterator.filter(_.windowStart == windowStart)

    WindowSummary(
      windowStart = windowStart,
      lastWindowUpdate = now,
      tweets = tweets + tweetExtracts.size,

      tweetsWithEmoji = tweetsWithEmoji + tweetsInThisWindow.count(_.containsEmoji),
      tweetsWithUrl = tweetsWithUrl + tweetsInThisWindow.count(_.containsUrl),
      tweetsWithPhotoUrl = tweetsWithPhotoUrl + tweetsInThisWindow.count(_.containsPhotoUrl),

      hashtagCounts = addCounts(hashtagCounts, tweetsInThisWindow.flatMap(_.hashTags).toVector),
      domainCounts = addCounts(domainCounts, tweetsInThisWindow.flatMap(_.urlDomains).toVector),
      emojiCounts = addCounts(emojiCounts, tweetsInThisWindow.flatMap(_.emojis).toVector)
    )
  }
}

object WindowSummary {

  def apply(windowStart: WindowStart, now: EpochMillis) =
    new WindowSummary(
      windowStart = windowStart,
      lastWindowUpdate = now,
      tweets = 0,
      tweetsWithEmoji = 0,
      tweetsWithUrl = 0,
      tweetsWithPhotoUrl = 0,
      hashtagCounts = Map.empty,
      domainCounts = Map.empty,
      emojiCounts = Map.empty
    )

  /** Calculate the count for each occurrence of a String */
  def occurrenceCounts(occurrences: Seq[String]): Map[String, Count] =
    occurrences.groupMapReduce(identity)(_ => 1L)(_ + _)

  /** Add the count of each occurrence of a key to the counts. */
  def addCounts(counts: Map[String, Count], occurrences: Seq[String]): Map[String, Count] =
    counts ++ occurrenceCounts(occurrences).map { case (occurrence, count) =>
      occurrence -> (count + counts.getOrElse(occurrence, 0L))
    }
}
