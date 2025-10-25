import avro.ElectronicOrder
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced

fun main() {
  val streamsProps = StreamsUtils.loadProperties()
  streamsProps[org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG] = "aggregations"
  streamsProps[org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name

  val configMap = StreamsUtils.propertiesToMap(streamsProps)

  val electronicSerde = StreamsUtils.getSpecificAvroSerde<ElectronicOrder>(configMap)
  val electronicOrderInputTopic =
    Pair(streamsProps.getProperty("aggregate.input.topic"), electronicSerde)
  val outputTopic = streamsProps.getProperty("aggregate.output.topic")

  val topology =
    Aggregations.createTopology(
      electronicOrderInputTopic,
      outputTopic
    ).build()

  StreamsUtils.runTopology(topology, streamsProps)
}

object Aggregations {
  fun createTopology(
    electronicOrderInputTopic: Pair<String, SpecificAvroSerde<ElectronicOrder>>,
    outputTopic: String
  ) = StreamsBuilder().apply {

    val electronicOrderStream =
      electronicOrderStream(electronicOrderInputTopic)

    val aggregation =
      { _: String, value: ElectronicOrder, aggregate: Double ->
        aggregate + value.price
      }
    val initializer = { 0.0 }

    electronicOrderStream.groupByKey().aggregate(
      initializer,
      aggregation,
      Materialized.with(Serdes.String(), Serdes.Double())
    ).toStream()
      .peek { key, value ->
        println("Aggregated Stream - Key: $key Value: $value")
      }
      .to(outputTopic, Produced.with(Serdes.String(), Serdes.Double()))
  }

  private fun StreamsBuilder.electronicOrderStream(inputTopic: Pair<String, SpecificAvroSerde<ElectronicOrder>>) =
    stream(inputTopic.first, Consumed.with(Serdes.String(), inputTopic.second))
      .peek { key, value ->
        println("ElectronicOrder Stream - Key: $key Value: $value")
      }
}
