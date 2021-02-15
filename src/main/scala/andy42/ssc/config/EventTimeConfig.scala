package andy42.ssc.config

import andy42.ssc.DurationMillis

import scala.concurrent.duration.FiniteDuration


case class EventTimeConfig(windowSize: FiniteDuration,
                           watermark: FiniteDuration) {

  val windowSizeMs: DurationMillis = windowSize.toMillis
  val watermarkMs: DurationMillis = watermark.toMillis
}
