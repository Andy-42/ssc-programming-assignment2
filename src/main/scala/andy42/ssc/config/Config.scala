package andy42.ssc.config

import pureconfig._
import pureconfig.generic.auto._

case class Config(twitterStream: TwitterStreamConfig,
                  eventTime: EventTimeConfig,
                  streamParameters: StreamParametersConfig,
                  summaryOutput: SummaryOutputConfig)

object Config {

  val config: Config = Config(
    twitterStream = ConfigSource.default
      .at(namespace = "twitter-stream")
      .loadOrThrow[TwitterStreamConfig],
    eventTime = ConfigSource.default
      .at(namespace = "event-time")
      .loadOrThrow[EventTimeConfig],
    streamParameters = ConfigSource.default
      .at(namespace = "stream-parameters")
      .loadOrThrow[StreamParametersConfig],
    summaryOutput = ConfigSource.default
      .at(namespace = "summary-output")
      .loadOrThrow[SummaryOutputConfig]
  )
}
