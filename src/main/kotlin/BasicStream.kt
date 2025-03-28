import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed

fun main() {
  val streamsProps = StreamsUtils.loadProperties()
  streamsProps[StreamsConfig.APPLICATION_ID_CONFIG] = "basic-streams"
  streamsProps[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
  streamsProps[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name

  val topology = BasicStream.createTopology(
    streamsProps.getProperty("basic.input.topic"),
    streamsProps.getProperty("basic.output.topic")
  ).build()

  StreamsUtils.runTopology(topology, streamsProps)
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
