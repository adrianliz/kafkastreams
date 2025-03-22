import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

val logger: Logger = LoggerFactory.getLogger("BasicStream")

fun main() {
  val streamsProps = StreamsUtils.loadProperties()
  streamsProps[StreamsConfig.APPLICATION_ID_CONFIG] = "basic-streams"
  streamsProps[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
  streamsProps[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name

  val topology = BasicStream.createTopology(
    streamsProps.getProperty("basic.input.topic"),
    streamsProps.getProperty("basic.output.topic")
  ).build()

  val shutdownLatch = CountDownLatch(1)
  KafkaStreams(topology, streamsProps).use { streams ->
    Runtime.getRuntime().addShutdownHook(Thread {
      streams.close(Duration.ofSeconds(2))
      shutdownLatch.countDown()
    })
    try {
      logger.info("Starting Kafka Streams basic application")
      streams.start()
      shutdownLatch.await()
    } catch (e: Throwable) {
      logger.error("Error running Kafka Streams application", e)
      exitProcess(1)
    }
  }

  logger.error("Kafka Streams application has closed")
  exitProcess(1)
}

object BasicStream {
  fun createTopology(inputTopic: String, outputTopic: String) = StreamsBuilder().apply {
    stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
      .peek { key, value -> println("Incoming record - key $key value $value") }
      .filter { _, value -> value.contains("orderNumber-") }
      .mapValues { value -> value.substring(value.indexOf("-") + 1) }
      .filter { _, value -> value.toLong() >= 1000 }
      .peek { key, value -> println("Outgoing record - key $key value $value") }
      .to(outputTopic)
  }
}
