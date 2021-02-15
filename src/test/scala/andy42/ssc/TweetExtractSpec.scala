package andy42.ssc

import io.circe._
import io.circe.parser._
import org.scalatest.flatspec._
import org.scalatest.matchers._
import andy42.ssc.config.EventTimeConfig
import cats.effect.IO
import fs2.Pure

import scala.concurrent.duration._

class TweetExtractSpec extends AnyFlatSpec with should.Matchers {

  val config: EventTimeConfig = EventTimeConfig(windowSize = 5.seconds, watermark = 15.seconds)
  val eventTime: EventTime = EventTime(config)
  val decode: Json => IO[fs2.Stream[Pure, TweetExtract]] = TweetExtract.decode[IO](eventTime = eventTime)

  "TweetExtract" should "decode valid JSON to Some(TweetExtract)" in {

    val rawJson: Json = parse(
      """
    {
      "created_at": "Tue Feb 27 21:11:40 +0000 2018",
      "text": "Hello World!"
    }
  """
    ).getOrElse(Json.Null)

    decode(rawJson).unsafeRunSync().toList shouldBe List(
      TweetExtract(windowStart = 1519765900000L,
        hashTags = Vector.empty,
        urlDomains = Vector.empty,
        emojis = Vector.empty))
  }

  it should "decode JSON with missing fields to None" in {
    val rawJson1: Json = parse(
      """
    {
      "xxx-created_at": "Tue Feb 27 21:11:40 +0000 2018",
      "text": "Hello World!"
    }
  """
    ).getOrElse(Json.Null)

    decode(rawJson1).unsafeRunSync().toList shouldBe Nil

    val rawJson2: Json = parse(
      """
    {
      "created_at": "123",
      "xxx-text": "Hello World!"
    }
  """
    ).getOrElse(Json.Null)

    decode(rawJson2).unsafeRunSync().toList shouldBe Nil
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

    decode(rawJson1).unsafeRunSync().toList shouldBe Nil
  }

  it should "move the createdAt to the start of that period" in {

    // FIXME: This test is dependent on the window-spec configuration, so need a better way to test

    val rawJson: Json = parse(
      """
    {
      "created_at": "Tue Feb 27 21:11:41 +0000 2018",
      "text": "Hello World!"
    }
  """
    ).getOrElse(Json.Null)

    decode(rawJson).unsafeRunSync().toList shouldBe List(
      // With the default configuration, createdAt would be 1519765901000 if WindowSpec.toWindowStart was not applied.
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

    decode(rawJson).unsafeRunSync().toList shouldBe List(
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

    decode(rawJson).unsafeRunSync().toList shouldBe List(
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

    decode(rawJson).unsafeRunSync().toList shouldBe List(
      TweetExtract(windowStart = 1519765900000L,
        hashTags = Vector("hello", "world"),
        urlDomains = Vector.empty,
        // Note that the order is reversed relative to the order in the original text
        emojis = Vector.empty))
  }
}
