package andy42.ssc.config

import pureconfig._
import pureconfig.generic.auto._


object Config {

  val TwitterStreamConfig: TwitterStream = ConfigSource.default
    .at(namespace = "twitter-stream")
    .loadOrThrow[TwitterStream]

  val EventTimeConfig: EventTime = ConfigSource.default
    .at(namespace = "event-time")
    .loadOrThrow[EventTime]

  val StreamParametersConfig: StreamParameters = ConfigSource.default
    .at(namespace = "stream-parameters")
    .loadOrThrow[StreamParameters]

  val SummaryOutputConfig: SummaryOutput = ConfigSource.default
    .at(namespace = "summary-output")
    .loadOrThrow[SummaryOutput]
}
