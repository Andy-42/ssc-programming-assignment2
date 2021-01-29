package andy42.ssc.config

import scala.concurrent.duration.FiniteDuration


case class StreamParameters(extractConcurrency: Int,
                            chunkSizeLimit: Int,
                            chunkGroupTimeout: FiniteDuration)
