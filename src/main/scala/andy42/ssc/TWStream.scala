package andy42.ssc

import org.http4s._
import org.http4s.client.blaze._
import org.http4s.client.oauth1
import org.http4s.implicits._
import cats.effect._
import com.typesafe.config.{Config, ConfigFactory}
import fs2.Stream
import io.circe.Json
import jawnfs2._
import org.typelevel.jawn.Facade

import scala.concurrent.ExecutionContext.global

/** Produces a Stream of JSON from the Twitter sample stream.
  * This uses the sample stream from the Twitter 1.1 API.
  * There is a V2 version of this Twitter API, but the http4s API does not yet support
  * the ability to sign OAuth 2 requests, so this implementation uses the Twitter 1.1 API.
  */
class TWStream[F[_] : ConcurrentEffect : ContextShift] {

  // jawn-fs2 needs to know that we want the JSON parsed to io.circe.Json
  implicit val f: Facade[Json] = new io.circe.jawn.CirceSupportParser(None, allowDuplicateKeys = false).facade

  /** Sign the request with OAuth 1.0a API key and access token.
    * OAuth signing is an effect due to generating a nonce for each `Request`.
    */
  def sign(req: Request[F]): F[Request[F]] = {
    val consumer = oauth1.Consumer(TWStream.ConsumerKey, TWStream.ConsumerSecret)
    val token = oauth1.Token(TWStream.AccessToken, TWStream.AccessTokenSecret)
    oauth1.signRequest(req, consumer, callback = None, verifier = None, token = Some(token))
  }

  /** Create a Stream of io.circe.Json from the Twitter sample stream. */
  def jsonStream(): Stream[F, Json] =
    for {
      // Create an HTTP client
      client <- BlazeClientBuilder(global).stream

      // Create a `Request[F]` and sign it with OAuth credentials
      request = Request[F](Method.GET, uri = uri"https://stream.twitter.com/1.1/statuses/sample.json")
      // `sign` returns a `F`, so we need to `Stream.eval` it to use a for-comprehension.
      signedRequest <- Stream.eval(sign(request))

      // Parse the response stream using jawn, producing a Stream of io.circe.Json
      res <- client.stream(signedRequest).flatMap(_.body.chunks.parseJsonStream)
    } yield res
}

object TWStream {
  val config: Config = ConfigFactory.load("application.conf").getConfig("twitter-stream")

  val ConsumerKey: String = config.getString("api-key")
  val ConsumerSecret: String = config.getString("api-key-secret")
  val AccessToken: String = config.getString("access-token")
  val AccessTokenSecret: String = config.getString("access-token-secret")
}