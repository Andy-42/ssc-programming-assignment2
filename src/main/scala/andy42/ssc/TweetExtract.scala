package andy42.ssc

import cats.effect.IO
import com.twitter.twittertext.{Extractor, TwitterTextEmojiRegex}
import fs2.{Pure, Stream}

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex
import scala.util.{Either, Try}


/** Data extracted from a tweet.
  *
  * @param windowStart The `created_at` timestamp from the tweet, in milliseconds, aligned to the
  *                    start of a summary window. The resolution of a tweet is one second.
  * @param hashTags    The hashtags extracted from the tweet text. The hashtag casing is exactly as it
  *                    it represented in the tweet (i.e., no transformation of the casing).
  *                    A hashtag can occur multiple times in one tweet.
  * @param emojis      The emoji extracted from a tweet text. An emoji can occur multiple times in one tweet.
  *                    An emoji can consist of multiple code points.
  * @param urlDomains  The domains (the host part of HTTP URL references) extracted from one tweet.
  *                    A domain can occur multiple times within one tweet.
  */
case class TweetExtract(windowStart: WindowStart,
                        hashTags: Vector[String],
                        emojis: Vector[String],
                        urlDomains: Vector[String]) {

  def containsEmoji: Boolean = emojis.nonEmpty

  def containsUrl: Boolean = urlDomains.nonEmpty

  def containsPhotoUrl: Boolean = urlDomains.exists(TweetExtract.isPhotoDomain)
}

object TweetExtract {

  /** Decode tweet JSON to a TweetExtract.
    *
    * A tweet is silently discarded if:
    *  - The created_at or text field is missing.
    *  - The created_at field cannot be decoded to an instant.
    *
    * In a fuller implementation, we would probably want to collect statistics on
    * the fraction of tweets that do not decode, but for simplicity, this implementation
    * simply drops them.
    *
    * This uses the Twitter library for extracting emoji, URLs and hashtags from tweet text.
    * This implementation is based on regular expressions.
    *
    * @param json The `io.circe.Json` instance to decode.
    * @return If the tweet decoded successfully, a singleton Stream, otherwise an empty Stream.
    */
  def decode[F[_]](json: io.circe.Json): IO[Stream[Pure, TweetExtract]] = IO {

    val hCursor = json.hcursor

    val decodeResult = for {
      createdAt: String <- hCursor.get[String]("created_at")
      text: String <- hCursor.get[String]("text")
      parsedDate <- parseDate(createdAt)
    } yield
      TweetExtract(
        windowStart = EventTime.toWindowStart(parsedDate),
        hashTags = extractHashTags(text),
        emojis = extractEmojis(text),
        urlDomains = extractUrlDomains(text)
      )

    decodeResult.fold(_ => Stream.empty, tweetExtract => Stream.emit(tweetExtract))
  }

  // For decoding tweet timestamps.
  private[this] val formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH)

  // Use the twitter text library Extractor to extract hashtags from text
  private[this] val extractor = new Extractor()

  // Use the twitter text library pattern for extracting emoji from text
  private[this] val emojiRegex = new Regex(TwitterTextEmojiRegex.VALID_EMOJI_PATTERN.pattern)

  def parseDate(dateString: String): Either[Throwable, EpochMillis] =
    Try(Instant.from(formatter.parse(dateString)).toEpochMilli).toEither

  def extractHashTags(text: String): Vector[String] = extractor.extractHashtags(text).asScala.toVector

  def extractEmojis(text: String): Vector[String] = emojiRegex.findAllIn(text).toVector

  /** Extract the domain (the host) from an URL
    *
    * While we would expect that all URLs will be valid (because they have already been
    * validated by the extraction regular expression), if an URL cannot be parsed at this point,
    * it is silently discarded.
    */
  def extractUrlDomains(text: String): Vector[String] =
    (for {
      urlString <- extractor.extractURLs(text).asScala
      url <- Try(new java.net.URL(urlString)).toOption
    } yield url.getHost).toVector

  def isPhotoDomain(domain: String): Boolean =
    domain == "www.instagram.com" || domain == "pic.twitter.com"
}
