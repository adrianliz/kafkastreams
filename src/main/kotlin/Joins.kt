import avro.ApplianceOrder
import avro.CombinedOrder
import avro.ElectronicOrder
import avro.User
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.kstream.Joined
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.StreamJoined
import org.apache.kafka.streams.kstream.ValueJoiner
import java.time.Duration
import java.time.Instant

fun main() {
  val streamsProps = StreamsUtils.loadProperties()
  streamsProps[StreamsConfig.APPLICATION_ID_CONFIG] = "joins"
  streamsProps[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name

  val configMap = StreamsUtils.propertiesToMap(streamsProps)
  val applianceSerde = StreamsUtils.getSpecificAvroSerde<ApplianceOrder>(configMap)
  val electronicSerde = StreamsUtils.getSpecificAvroSerde<ElectronicOrder>(configMap)
  val userSerde = StreamsUtils.getSpecificAvroSerde<User>(configMap)
  val combinedSerde = StreamsUtils.getSpecificAvroSerde<CombinedOrder>(configMap)

  val applianceInput =
    Pair(streamsProps.getProperty("stream_one.input.topic"), applianceSerde)
  val electronicInput =
    Pair(streamsProps.getProperty("stream_two.input.topic"), electronicSerde)
  val userInput = Pair(streamsProps.getProperty("table.input.topic"), userSerde)
  val outputTopic = streamsProps.getProperty("joins.output.topic")

  val topology =
    Joins.createTopology(
      applianceInput,
      electronicInput,
      userInput,
      combinedSerde,
      outputTopic
    )
  StreamsUtils.runTopology(topology, streamsProps)
}

object Joins {
  fun createTopology(
    applianceInput: Pair<String, SpecificAvroSerde<ApplianceOrder>>,
    electronicInput: Pair<String, SpecificAvroSerde<ElectronicOrder>>,
    userInput: Pair<String, SpecificAvroSerde<User>>,
    combinedSerde: SpecificAvroSerde<CombinedOrder>,
    outputTopic: String
  ): Topology {
    val builder = StreamsBuilder()
    val applianceStream =
      builder.applianceStream(applianceInput).selectKey { _, value -> value.userId }
    val electronicStream =
      builder.electronicStream(electronicInput).selectKey { _, value -> value.userId }

    val combinedStream =
      applianceStream.join(
        electronicStream,
        ValueJoiner { applianceOrder: ApplianceOrder, electronicOrder: ElectronicOrder ->
          CombinedOrder.newBuilder()
            .setApplianceOrderId(applianceOrder.orderId)
            .setApplianceId(applianceOrder.applianceId)
            .setElectronicOrderId(electronicOrder.orderId)
            .setTime(Instant.now().toEpochMilli())
            .build()
        },
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(30)),
        StreamJoined.with(Serdes.String(), applianceInput.second, electronicInput.second)
      ).peek { key, value -> println("Stream-Stream Join record key $key value $value") }

    combinedStream.leftJoin(
      builder.userTable(userInput),
      ValueJoiner { combinedOrder: CombinedOrder, user: User? ->
        if (user != null) {
          combinedOrder.apply { user_name = user.name }
        } else {
          combinedOrder
        }
      },
      Joined.with(Serdes.String(), combinedSerde, userInput.second)
    )
      .peek { key, value -> println("Stream-Table Join record key $key value $value") }

    combinedStream.to(outputTopic, Produced.with(Serdes.String(), combinedSerde))
    return builder.build()
  }

  private fun StreamsBuilder.electronicStream(electronicInput: Pair<String, SpecificAvroSerde<ElectronicOrder>>) =
    stream(electronicInput.first, Consumed.with(Serdes.String(), electronicInput.second))
      .peek { key, value -> println("Electronic stream incoming record $key value $value") }

  private fun StreamsBuilder.applianceStream(applianceInput: Pair<String, SpecificAvroSerde<ApplianceOrder>>) =
    stream(applianceInput.first, Consumed.with(Serdes.String(), applianceInput.second))
      .peek { key, value -> println("Appliance stream incoming record $key value $value") }

  private fun StreamsBuilder.userTable(userInput: Pair<String, SpecificAvroSerde<User>>) =
    table(userInput.first, Materialized.with(Serdes.String(), userInput.second))
}
