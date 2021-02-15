package andy42.ssc

import andy42.ssc.config.EventTimeConfig
import org.scalatest.flatspec._
import org.scalatest.matchers._

import scala.concurrent.duration._

/** Tests for EventTime calculations.
  *
  * TODO: EventTime was changed from using config directly to being parameterized explicitly using a config,
  * TODO: so change these tests to use config.
  */
class EventTimeSpec extends AnyFlatSpec with should.Matchers {

  val config = EventTimeConfig(windowSize = 5.seconds, watermark = 15.seconds)
  val eventTime = EventTime(config)

  "EventTime window alignment" should "adjust (millisecond) times to the start of some window" in {

    val initialRaw = 1611856011234L // A value already adjusted to the start of window
    val initial = eventTime.toWindowStart(initialRaw)

    // Adjusting a value that is already adjusted to the window start is a no-op
    eventTime.toWindowStart(initial) shouldBe initial

    // The previous tick is in the previous window
    eventTime.toWindowStart(initial - 1) shouldBe initial - config.windowSizeMs

    // Check when the window start calculation ticks over to the next window
    eventTime.toWindowStart(initial + config.windowSizeMs - 1) shouldBe initial
    eventTime.toWindowStart(initial + config.windowSizeMs) shouldBe initial + config.windowSizeMs

    // toWindowEnd aligns a time to the last tick in that window
    eventTime.toWindowEnd(initialRaw) shouldBe initial + config.windowSizeMs - 1
  }

  "EventTime.isExpired" should "determine if a point in time is expired relative to the watermark" in {
    // TODO
  }
}
