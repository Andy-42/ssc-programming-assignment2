package andy42.ssc.config

import scala.concurrent.duration.FiniteDuration


case class EventTime(windowSize: FiniteDuration,
                     watermark: FiniteDuration) {

  val windowSizeMs = windowSize.toMillis
  val watermarkMs = watermark.toMillis
}
