package andy42.ssc

import fs2.Chunk


/** The summary of tweets within a given window.
  *
  * @param createdAt          The time that the tweet was created, adjusted to the start of the window
  *                           that the original created_at falls into.
  * @param tweets             The count of the number of tweets in this window.
  * @param tweetsWithEmoji    The count of tweets in this window that contain at least one emoji in the text.
  * @param tweetsWithUrl      The count of tweets in this window that contain at least one URL in the text.
  * @param tweetsWithPhotoUrl The count of tweets in this window that contain at least one photo URL in the text.
  * @param hashtagCounts      The count of the number of times each hashtag occurs in tweets in this window.
  * @param domainCounts       The count of the number of times each domain occurs in tweets in this window.
  * @param emojiCounts        The count of the number of times each emoji (or emoji sequence) occurs in this window.
  */
case class WindowSummary(createdAt: Long,
                         tweets: Long,
                         tweetsWithEmoji: Long,
                         tweetsWithUrl: Long,
                         tweetsWithPhotoUrl: Long,
                         hashtagCounts: Map[String, Long],
                         domainCounts: Map[String, Long],
                         emojiCounts: Map[String, Long]) {

  import WindowSummary.addCounts

  /** Add the occurrences from a Chunk[TweetExtract] to a existing WindowSummary */
  def add(tweetExtracts: Chunk[TweetExtract]): WindowSummary = {

    def tweetsInThisWindow: Iterator[TweetExtract] = tweetExtracts.iterator.filter(_.createdAt == createdAt)

    WindowSummary(
      createdAt = createdAt,
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

  def apply(createdAt: Long) =
    new WindowSummary(
      createdAt = createdAt,
      tweets = 0,
      tweetsWithEmoji = 0,
      tweetsWithUrl = 0,
      tweetsWithPhotoUrl = 0,
      hashtagCounts = Map.empty,
      domainCounts = Map.empty,
      emojiCounts = Map.empty
    )

  /** Calculate the count for each occurrence of a String */
  def occurrenceCounts(occurrences: Seq[String]): Map[String, Long] =
    occurrences.groupMapReduce(identity)(_ => 1L)(_ + _)

  /** Add the count of each occurrence of a key to the counts. */
  def addCounts(counts: Map[String, Long], occurrences: Seq[String]): Map[String, Long] = {

    val updatedCounts = occurrenceCounts(occurrences).map { case (key, count) =>
      key -> (count + counts.getOrElse(key, 0L))
    }

    counts ++ updatedCounts
  }
}
