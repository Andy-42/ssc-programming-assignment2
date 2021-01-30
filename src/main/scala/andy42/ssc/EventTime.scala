package andy42.ssc

import config.Config.{EventTimeConfig => Config}
 

/** Functions for segmenting an event stream into tumbling windows.
  *
  * As events flow in, the Tweet's `created_at` timestamp is immediately mapped to
  * a window using `toWindowStart`. All summary calculations downstream use this
  * window time.
  *
  * The `isExpired` function uses a watermark, which is the length of time that
  * we will wait for tweets to arrive. If they arrive after they are expired, they are
  * discarded. The summary statistics for a window will only be emitted to the output
  * stream after the corresponding window has expired.
  */
object EventTime {

  /** Move an instant (in millis) to the beginning of a Window */
  def toWindowStart(createdAt: EpochMillis): WindowStart = createdAt - (createdAt % Config.windowSizeMs)

  def toWindowEnd(createdAt: EpochMillis): EpochMillis = toWindowStart(createdAt) + Config.windowSizeMs - 1

  /** Does an instant (in millis) fall into a fully-expired window?
    * We compare the instant that the window ends to the watermark position (relative to now):
    * if the end of the window is before the watermark, that window is fully expired.
    */
  def isExpired(createdAt: EpochMillis, now: EpochMillis): Boolean =
    toWindowEnd(createdAt) < (now - Config.watermarkMs)
}
