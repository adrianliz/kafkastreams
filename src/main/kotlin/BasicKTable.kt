import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.KeyValueStore

fun main() {
  val streamsProps = StreamsUtils.loadProperties()
  streamsProps[StreamsConfig.APPLICATION_ID_CONFIG] = "basic-ktable"
  streamsProps[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
  streamsProps[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name

  val topology = BasicKTable.createTopology(
    streamsProps.getProperty("basic.input.topic"),
    streamsProps.getProperty("basic.output.topic")
  ).build()

  StreamsUtils.runTopology(topology, streamsProps)
}

object BasicKTable {
  fun createTopology(inputTopic: String, outputTopic: String) = StreamsBuilder().apply {
    table(
      inputTopic,
      Materialized.`as`<String, String, KeyValueStore<Bytes, ByteArray>>("ktable-store")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.String())
    )
      .filter { _, value -> value.contains("orderNumber-") }
      .mapValues { value -> value.substring(value.indexOf("-") + 1) }
      .filter { _, value -> value.toLong() >= 1000 }
      .toStream()
      .peek { key, value -> println("Outgoing record - key $key value $value") }
      .to(outputTopic)
  }
}
