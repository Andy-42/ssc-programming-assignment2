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

  /** Add the occurrences from a single TweetExtract to an existing WindowSummary */
  def add(tweetExtract: TweetExtract): WindowSummary = {

    require(tweetExtract.createdAt == createdAt,
      "The extract must have the same createdAt as this WindowSummary")

    WindowSummary(
      createdAt = createdAt,
      tweets = tweets + 1,

      tweetsWithEmoji = tweetsWithEmoji + (if (tweetExtract.containsEmoji) 1 else 0),
      tweetsWithUrl = tweetsWithUrl + (if (tweetExtract.containsUrl) 1 else 0),
      tweetsWithPhotoUrl = tweetsWithPhotoUrl + (if (tweetExtract.containsPhotoUrl) 1 else 0),

      hashtagCounts = addCounts(hashtagCounts, tweetExtract.hashTags),
      domainCounts = addCounts(domainCounts, tweetExtract.urlDomains),
      emojiCounts = addCounts(emojiCounts, tweetExtract.emojis))
  }

  /** Add the occurrences from a Chunk[TweetExtract to a existing WindowSummary */
  def add(tweetExtracts: Chunk[TweetExtract]): WindowSummary = {

    require(tweetExtracts.forall(_.createdAt == createdAt),
      "All extracts must have the same createdAt as this WindowSummary")

    WindowSummary(
      createdAt = createdAt,
      tweets = tweets + tweetExtracts.size,

      tweetsWithEmoji = tweetsWithEmoji + tweetExtracts.iterator.count(_.containsEmoji),
      tweetsWithUrl = tweetsWithUrl + tweetExtracts.iterator.count(_.containsUrl),
      tweetsWithPhotoUrl = tweetsWithPhotoUrl + tweetExtracts.iterator.count(_.containsPhotoUrl),

      hashtagCounts = addCounts(hashtagCounts, tweetExtracts.iterator.flatMap(_.hashTags).toVector),
      domainCounts = addCounts(domainCounts, tweetExtracts.iterator.flatMap(_.urlDomains).toVector),
      emojiCounts = addCounts(emojiCounts, tweetExtracts.iterator.flatMap(_.emojis).toVector)
    )
  }
}

object WindowSummary {

  /** Create a new WindowSummary from a TweetExtract. */
  def apply(tweetExtract: TweetExtract): WindowSummary =
    WindowSummary(
      createdAt = tweetExtract.createdAt,
      tweets = 1,
      tweetsWithEmoji = if (tweetExtract.containsEmoji) 1 else 0,
      tweetsWithUrl = if (tweetExtract.containsUrl) 1 else 0,
      tweetsWithPhotoUrl = if (tweetExtract.containsPhotoUrl) 1 else 0,
      hashtagCounts = occurrenceCounts(tweetExtract.hashTags),
      domainCounts = occurrenceCounts(tweetExtract.urlDomains),
      emojiCounts = occurrenceCounts(tweetExtract.emojis)
    )

  /** Create a new WindowSummary from a Chunk[TweetExtract] */
  def apply(tweetExtracts: Chunk[TweetExtract]): WindowSummary = {

    require(tweetExtracts.nonEmpty)
    val createdAt = tweetExtracts(0).createdAt
    require(tweetExtracts.forall(_.createdAt == createdAt), "All extracts must have the same createdAt")

    WindowSummary(
      createdAt = createdAt,
      tweets = tweetExtracts.size.toLong,
      tweetsWithEmoji = tweetExtracts.iterator.count(_.containsEmoji).toLong,
      tweetsWithUrl = tweetExtracts.iterator.count(_.containsUrl).toLong,
      tweetsWithPhotoUrl = tweetExtracts.iterator.count(_.containsPhotoUrl).toLong,
      hashtagCounts = occurrenceCounts(tweetExtracts.iterator.flatMap(_.hashTags).toVector),
      domainCounts = occurrenceCounts(tweetExtracts.iterator.flatMap(_.urlDomains).toVector),
      emojiCounts = occurrenceCounts(tweetExtracts.iterator.flatMap(_.emojis).toVector)
    )
  }

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

