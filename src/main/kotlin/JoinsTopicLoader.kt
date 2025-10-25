
import avro.ApplianceOrder
import avro.ElectronicOrder
import avro.User
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Instant
import java.util.Properties

fun main() {
  val properties = StreamsUtils.loadProperties()
  properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
  properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java
  JoinsTopicLoader.runProducer(properties)
}

object JoinsTopicLoader {
  fun runProducer(properties: Properties) {
    val callback = StreamsUtils.producerCallback()
    Admin.create(properties).use { adminClient ->
      KafkaProducer<String, SpecificRecord>(properties).use { producer ->
        val leftSideTopic = properties.getProperty("stream_one.input.topic");
        val rightSideTopic = properties.getProperty("stream_two.input.topic");
        val tableTopic = properties.getProperty("table.input.topic");
        val outputTopic = properties.getProperty("joins.output.topic");

        val topics = listOf(
          StreamsUtils.createTopic(leftSideTopic),
          StreamsUtils.createTopic(rightSideTopic),
          StreamsUtils.createTopic(tableTopic),
          StreamsUtils.createTopic(outputTopic)
        )
        adminClient.createTopics(topics)

        val applianceOrderOne = ApplianceOrder.newBuilder()
          .setApplianceId("dishwasher-1333")
          .setOrderId("remodel-1")
          .setUserId("10261998")
          .setTime(Instant.now().toEpochMilli()).build()

        val applianceOrderTwo = ApplianceOrder.newBuilder()
          .setApplianceId("stove-2333")
          .setOrderId("remodel-2")
          .setUserId("10261999")
          .setTime(Instant.now().toEpochMilli()).build()
        val applianceOrders = listOf(applianceOrderOne, applianceOrderTwo)

        val electronicOrderOne = ElectronicOrder.newBuilder()
          .setElectronicId("television-2333")
          .setOrderId("remodel-1")
          .setUserId("10261998")
          .setTime(Instant.now().toEpochMilli()).build()

        val electronicOrderTwo = ElectronicOrder.newBuilder()
          .setElectronicId("laptop-5333")
          .setOrderId("remodel-2")
          .setUserId("10261999")
          .setTime(Instant.now().toEpochMilli()).build()

        val electronicOrders = listOf(electronicOrderOne, electronicOrderTwo)

        val userOne = User.newBuilder().setUserId("10261998").setAddress("5405 6th Avenue")
          .setName("Elizabeth Jones").build()
        val userTwo = User.newBuilder().setUserId("10261999").setAddress("407 64th Street")
          .setName("Art Vandelay").build()
        val users = listOf(userOne, userTwo)

        users.forEach { user ->
          val producerRecord = ProducerRecord<String, SpecificRecord>(tableTopic, user.userId, user)
          producer.send(producerRecord, callback)
        }

        applianceOrders.forEach { applianceOrder ->
          val producerRecord = ProducerRecord<String, SpecificRecord>(
            leftSideTopic,
            applianceOrder.applianceId,
            applianceOrder
          )
          producer.send(producerRecord, callback)
        }

        electronicOrders.forEach { electronicOrder ->
          val producerRecord = ProducerRecord<String, SpecificRecord>(
            rightSideTopic,
            electronicOrder.electronicId,
            electronicOrder
          )
          producer.send(producerRecord, callback)
        }
      }
    }
  }
}
