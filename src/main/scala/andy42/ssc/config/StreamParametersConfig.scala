package andy42.ssc.config

import scala.concurrent.duration.FiniteDuration


case class StreamParametersConfig(extractConcurrency: Int,
                                  chunkSizeLimit: Int,
                                  chunkGroupTimeout: FiniteDuration)
