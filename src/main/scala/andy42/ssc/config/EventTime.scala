package andy42.ssc.config

import scala.concurrent.duration.FiniteDuration


case class EventTime(windowSize: FiniteDuration,
                     watermark: FiniteDuration) {

  val windowSizeMs: Long = windowSize.toMillis
  val watermarkMs: Long = watermark.toMillis
}
