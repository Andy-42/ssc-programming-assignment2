package andy42.ssc

import com.twitter.twittertext.{Extractor, TwitterTextEmojiRegex}
import io.circe.HCursor

import java.net.URL
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

  def containsPhotoUrl: Boolean = urlDomains.exists(TweetExtract.photoDomains.contains)
}

object TweetExtract {

  /** Decode tweet JSON to either a TweetExtract, or if decoding fails, a reason for the decode failure.
    *
    * Decoding could fail because:
    *  - The created_at or text field is missing.
    *  - The created_at field cannot be decoded to an instant.
    *  - An URL extracted from the text could not be parsed.
    *
    * This uses the Twitter library for extracting emoji, URLs and hashtags from tweet text.
    * This implementation is based on regular expressions.
    *
    * @param eventTime A configured instance of EventTime.
    * @param json      The `io.circe.Json` instance to decode.
    * @return Either an decode failure reason or the extracted tweet.
    */
  def decode(eventTime: EventTime)(json: io.circe.Json): Either[String, TweetExtract] = {

    implicit val hCursor: HCursor = json.hcursor

    for {
      createdAt <- getStringField("created_at")
      text <- getStringField("text")
      parsedDate <- parseDate(createdAt)
      urlDomains <- parseUrlDomains(extractUrls(text)) // Left if parsing any URL fails
    } yield TweetExtract(
      windowStart = eventTime.toWindowStart(parsedDate),
      hashTags = extractHashTags(text),
      emojis = extractEmojis(text),
      urlDomains = urlDomains
    )
  }

  private def getStringField(name: String)(implicit hCursor: HCursor): Either[String, String] =
    hCursor.get[String](name)
      .left.map(_ => s"get $name")

  // For decoding tweet timestamps.
  private[this] val formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH)

  def parseDate(dateString: String): Either[String, EpochMillis] =
    Try(Instant.from(formatter.parse(dateString)).toEpochMilli)
      .toEither.left.map(_ => s"parseDate")


  // Use the twitter text library Extractor to extract hashtags and URLs from text
  private[this] val extractor = new Extractor()

  // Use the twitter text library pattern for extracting emoji from text
  private[this] val emojiRegex = new Regex(TwitterTextEmojiRegex.VALID_EMOJI_PATTERN.pattern)

  def extractHashTags(text: String): Vector[String] = extractor.extractHashtags(text).asScala.toVector

  def extractEmojis(text: String): Vector[String] = emojiRegex.findAllIn(text).toVector

  /** Extract URLs from `text`, and then the domains (host) from the URL text.
    *
    * While we would expect that all URLs will be valid (because they have already been
    * validated by the extraction regular expression), we will fail here if any URL that
    * has been extracted can't be parsed.
    */
  def extractUrls(text: String): Vector[String] =
    extractor.extractURLs(text).asScala.toVector

  def parseUrlDomains(urlStrings: Vector[String]): Either[String, Vector[String]] = {

    val maybeURLs = urlStrings.map(urlString => Try(new URL(urlString)))

    if (maybeURLs.exists(_.isFailure))
      Left("parseUrl")
    else
      Right(maybeURLs.map(_.get.getHost))
  }

  private val photoDomains = Set("www.instagram.com", "pic.twitter.com")
}
