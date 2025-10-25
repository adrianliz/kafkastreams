
import avro.ElectronicOrder
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Instant
import java.util.Properties
import java.util.function.Consumer

fun main() {
  val properties = StreamsUtils.loadProperties()
  properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
  properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java
  AggregationsTopicLoader.runProducer(properties)
}

object AggregationsTopicLoader {
  fun runProducer(properties: Properties) {
    val callback = StreamsUtils.producerCallback()

    Admin.create(properties).use { adminClient ->
      val inputTopic = properties.getProperty("aggregate.input.topic");
      val outputTopic = properties.getProperty("aggregate.output.topic");
      val topics = listOf(
        StreamsUtils.createTopic(inputTopic),
        StreamsUtils.createTopic(outputTopic)
      )
      adminClient.createTopics(topics)

      KafkaProducer<String, ElectronicOrder>(properties).use { producer ->

        val instant = Instant.now()
        val electronicOrderOne = ElectronicOrder.newBuilder()
          .setElectronicId("HDTV-2333")
          .setOrderId("instore-1")
          .setUserId("10261998")
          .setPrice(2000.00)
          .setTime(instant.toEpochMilli()).build()

        val electronicOrderTwo = ElectronicOrder.newBuilder()
          .setElectronicId("HDTV-2333")
          .setOrderId("instore-1")
          .setUserId("1033737373")
          .setPrice(1999.23)
          .setTime(instant.plusSeconds(10L).toEpochMilli()).build()

        val electronicOrderThree = ElectronicOrder.newBuilder()
          .setElectronicId("HDTV-2333")
          .setOrderId("instore-1")
          .setUserId("1026333")
          .setPrice(4500.00)
          .setTime(instant.plusSeconds(10L).toEpochMilli()).build()

        val electronicOrderFour = ElectronicOrder.newBuilder()
          .setElectronicId("HDTV-2333")
          .setOrderId("instore-1")
          .setUserId("1038884844")
          .setPrice(1333.98)
          .setTime(instant.plusSeconds(102L).toEpochMilli()).build()
        val electronicOrders =
          listOf(
            electronicOrderOne,
            electronicOrderTwo,
            electronicOrderThree,
            electronicOrderFour
          )

        electronicOrders.forEach((Consumer { electronicOrder ->
          val producerRecord = ProducerRecord(
            inputTopic,
            0,
            electronicOrder.getTime(),
            electronicOrder.electronicId,
            electronicOrder
          )
          producer.send(producerRecord, callback)
        }))
      }
    }
  }
}
