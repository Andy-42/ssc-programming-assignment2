package andy42.ssc

import andy42.ssc.config.EventTimeConfig
import io.circe._
import io.circe.parser._
import org.scalatest.flatspec._
import org.scalatest.matchers._

import scala.concurrent.duration._


class TweetExtractSpec extends AnyFlatSpec with should.Matchers {

  val config: EventTimeConfig = EventTimeConfig(windowSize = 5.seconds, watermark = 15.seconds)
  val eventTime: EventTime = EventTime(config)
  val decode: Json => Either[String, TweetExtract] = TweetExtract.decodeToEither(eventTime = eventTime)

  "TweetExtract" should "decode valid JSON to Some(TweetExtract)" in {

    val rawJson: Json = parse(
      """
    {
      "created_at": "Tue Feb 27 21:11:40 +0000 2018",
      "text": "Hello World!"
    }
  """
    ).getOrElse(Json.Null)

    decode(rawJson) shouldBe Right(
      TweetExtract(windowStart = 1519765900000L,
        hashTags = Vector.empty,
        urlDomains = Vector.empty,
        emojis = Vector.empty))
  }

  it should "decode JSON with missing fields to a Left" in {
    val rawJson1: Json = parse(
      """
    {
      "xxx-created_at": "Tue Feb 27 21:11:40 +0000 2018",
      "text": "Hello World!"
    }
  """
    ).getOrElse(Json.Null)

    decode(rawJson1) shouldBe Left("get created_at")

    val rawJson2: Json = parse(
      """
    {
      "created_at": "123",
      "xxx-text": "Hello World!"
    }
  """
    ).getOrElse(Json.Null)

    decode(rawJson2) shouldBe Left("get text")
  }

  it should "decode JSON with an invalid created_at value to None" in {
    val rawJson1: Json = parse(
      """
    {
      "created_at": "Tue XXX 27 21:11:40 +0000 2018",
      "text": "Hello World!"
    }
  """
    ).getOrElse(Json.Null)

    decode(rawJson1) shouldBe Left("parseDate")
  }

  it should "move the createdAt to the start of that period" in {

    val rawJson: Json = parse(
      """
    {
      "created_at": "Tue Feb 27 21:11:41 +0000 2018",
      "text": "Hello World!"
    }
  """
    ).getOrElse(Json.Null)

    decode(rawJson) shouldBe Right(
      // Before mapping to start of window, createdAt would be 1519765901000.
      TweetExtract(windowStart = 1519765900000L,
        hashTags = Vector.empty,
        urlDomains = Vector.empty,
        // Note that the order is reversed relative to the order in the original text
        emojis = Vector.empty))

  }

  it should "extract emoji from tweets" in {
    val rawJson: Json = parse(
      """
    {
      "created_at": "Tue Feb 27 21:11:40 +0000 2018",
      "text": "grinning face: 🤪; woman with headscarf: 🧕; woman with headscarf + medium-dark skin tone: 🧕🏾; flag (England): 🏴󠁧󠁢󠁥󠁮󠁧󠁿"
    }
  """
    ).getOrElse(Json.Null)

    decode(rawJson) shouldBe Right(
      TweetExtract(windowStart = 1519765900000L,
        hashTags = Vector.empty,
        urlDomains = Vector.empty,
        emojis = Vector("🤪", "🧕", "🧕🏾", "🏴󠁧󠁢󠁥󠁮󠁧󠁿")))
  }

  it should "extract URL domains from text" in {
    val rawJson: Json = parse(
      """
    {
      "created_at": "Tue Feb 27 21:11:40 +0000 2018",
      "text": "hello http://foo.com/bar?baz, world https://twitter.com/foo󠁿"
    }
  """
    ).getOrElse(Json.Null)

    decode(rawJson) shouldBe Right(
      TweetExtract(windowStart = 1519765900000L,
        hashTags = Vector.empty,
        urlDomains = Vector("foo.com", "twitter.com"),
        // Note that the order is reversed relative to the order in the original text
        emojis = Vector.empty))
  }

  it should "extract hashtags from text" in {
    val rawJson: Json = parse(
      """
    {
      "created_at": "Tue Feb 27 21:11:40 +0000 2018",
      "text": "#hello #world!󠁿"
    }
  """
    ).getOrElse(Json.Null)

    decode(rawJson) shouldBe Right(
      TweetExtract(windowStart = 1519765900000L,
        hashTags = Vector("hello", "world"),
        urlDomains = Vector.empty,
        // Note that the order is reversed relative to the order in the original text
        emojis = Vector.empty))
  }

  "extractUrlDomains" should "produce a Right only if all URL parse successfully" in {
    TweetExtract.parseUrlDomains(Vector("http://foo.com", "https://bar.com")) shouldBe
      Right(Vector("foo.com", "bar.com"))

    TweetExtract.parseUrlDomains(Vector("httpx://foo.com", "https://bar.com")) shouldBe
      Left("parseUrl")
  }
}
