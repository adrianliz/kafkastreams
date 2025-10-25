import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import io.github.cdimascio.dotenv.Dotenv
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.Topology
import java.io.FileInputStream
import java.time.Duration
import java.util.Properties
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess


object StreamsUtils {
  private const val PROPERTIES_FILE_PATH = "src/main/resources/streams.properties"
  private const val REPLICATION_FACTOR: Short = 3
  private const val PARTITIONS = 6

  private val dotenv = Dotenv.load()

  fun runTopology(topology: Topology, streamProps: Properties) {
    val shutdownLatch = CountDownLatch(1)
    KafkaStreams(topology, streamProps).use { streams ->
      Runtime.getRuntime().addShutdownHook(Thread {
        streams.close(Duration.ofSeconds(2))
        shutdownLatch.countDown()
      })
      try {
        streams.cleanUp()
        streams.start()
        shutdownLatch.await()
      } catch (e: Throwable) {
        exitProcess(1)
      }
    }
    exitProcess(1)
  }

  fun loadProperties() =
    Properties().apply {
      FileInputStream(PROPERTIES_FILE_PATH).use { load(it) }
      setProperty("bootstrap.servers", dotenv["BOOTSTRAP_SERVERS"])
      setProperty(
        "sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule " +
            "required username=\"${dotenv["KAFKA_USER"]}\" password=\"${dotenv["KAFKA_PASSWORD"]}\";"
      )
      setProperty("schema.registry.url", dotenv["SCHEMA_REGISTRY_URL"])
      setProperty(
        "basic.auth.user.info",
        "${dotenv["SCHEMA_REGISTRY_USER"]}:${dotenv["SCHEMA_REGISTRY_PASSWORD"]}"
      )
    }

  fun propertiesToMap(properties: Properties): MutableMap<String, Any> {
    val configs = HashMap<String, Any>()
    properties.forEach { (key, value) ->
      configs[key.toString()] = value!!
    }
    return configs
  }

  fun createTopic(topicName: String) = NewTopic(topicName, PARTITIONS, REPLICATION_FACTOR)

  fun <T : SpecificRecord> getSpecificAvroSerde(config: Map<String, Any>): SpecificAvroSerde<T> {
    val specificAvroSerde = SpecificAvroSerde<T>()
    specificAvroSerde.configure(config, false)
    return specificAvroSerde
  }
}
