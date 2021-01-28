package andy42.ssc

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.util.Try
import com.twitter.twittertext.{Extractor, TwitterTextEmojiRegex}

import scala.jdk.CollectionConverters._
import scala.annotation.tailrec
import fs2.{Pure, Stream}

/** Data extracted from a tweet.
  *
  * @param createdAt  The `created_at` timestamp from the tweet, in milliseconds, aligned to the
  *                   start of a summary window. The resolution of a tweet is one second.
  * @param hashTags   The hashtags extracted from the tweet text. The hashtag casing is exactly as it
  *                   it represented in the tweet (i.e., no transformation of the casing).
  *                   A hashtag can occur multiple times in one tweet.
  * @param emojis     The emoji extracted from a tweet text. An emoji can occur multiple times in one tweet.
  *                   An emoji can consist of multiple code points.
  * @param urlDomains The domains (the host part of HTTP URL references) extracted from one tweet.
  *                   A domain can occur multiple times within one tweet.
  */
case class TweetExtract(createdAt: Long,
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
    * @return The optional decoded tweet. A failure to decode the tweet results in `None`
    */
  def decode(json: io.circe.Json): Stream[Pure, TweetExtract] = {

    val hCursor = json.hcursor

    val decodeResult: Option[TweetExtract] = for {
      createdAt: String <- hCursor.get[String]("created_at").toOption
      text: String <- hCursor.get[String]("text").toOption
      parsedDate: Long <- parseDate(createdAt)
    } yield
      TweetExtract(
        createdAt = EventTime.toWindowStart(parsedDate),
        hashTags = extractHashTags(text),
        emojis = extractEmojis(text),
        urlDomains = extractUrlDomains(text)
      )

    // FIXME: Non-idiomatic Scala
    if (decodeResult.isEmpty)
      Stream.empty
    else
      Stream.emit(decodeResult.get)

    // TODO: Why doesn't this fold work?
//    decodeResult.fold(Stream.empty){ Stream.emit(_) }
  }

  // For decoding tweet timestamps.
  private[this] val formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH)

  // Extractor is from the Twitter-provided library for extracting from a Tweet's text
  private[this] val extractor = new Extractor()

  def parseDate(dateString: String): Option[Long] =
    Try(Instant.from(formatter.parse(dateString)).toEpochMilli).toOption

  def extractHashTags(text: String): Vector[String] = extractor.extractHashtags(text).asScala.toVector

  def extractEmojis(text: String): Vector[String] = {

    val matcher = TwitterTextEmojiRegex.VALID_EMOJI_PATTERN.matcher(text)

    @tailrec
    def accumulate(r: List[String] = Nil): List[String] =
      if (matcher.find())
        accumulate(r = matcher.group() :: r)
      else
        r

    accumulate().toVector
  }

  /** Extract the domain (the host) from an URL
    *
    * While we would expect that all URLs will be valid (because they have already been
    * validated by the extraction regular expression), if an URL cannot be parsed at this point,
    * it is silently discarded.
    */
  def extractUrlDomains(text: String): Vector[String] = {
    val urlDomains = for {
      urlString <- extractor.extractURLs(text).asScala
      url <- Try(new java.net.URL(urlString)).toOption
    } yield url.getHost

    urlDomains.toVector
  }

  def isPhotoDomain(domain: String): Boolean =
    domain == "www.instagram.com" || domain == "pic.twitter.com"
}