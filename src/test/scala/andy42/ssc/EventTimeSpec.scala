package andy42.ssc

import org.scalatest._
import flatspec._
import matchers._

/** Tests for EventTime calculations.
  */
class EventTimeSpec extends AnyFlatSpec with should.Matchers {

  "EventTime window alignment" should "adjust (millisecond) times to the start of some window" in {

    val initialRaw = 1611856011234L // A value already adjusted to the start of window
    val initial = EventTime.toWindowStart(initialRaw)

    // Adjusting a value that is already adjusted to the window start is a no-op
    EventTime.toWindowStart(initial) shouldBe initial

    // The previous tick is in the previous window
    EventTime.toWindowStart(initial - 1) shouldBe initial - EventTime.WindowSize

    // Check when the window start calculation ticks over to the next window
    EventTime.toWindowStart(initial + EventTime.WindowSize - 1) shouldBe initial
    EventTime.toWindowStart(initial + EventTime.WindowSize) shouldBe initial + EventTime.WindowSize

    // toWindowEnd aligns a time to the last tick in that window
    EventTime.toWindowEnd(initialRaw) shouldBe initial + EventTime.WindowSize + 1
  }

  "EventTime.isExpired" should "determine if a point in time is expired relative to the watermark" in {
    // TODO
  }
}
