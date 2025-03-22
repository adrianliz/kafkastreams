import io.github.cdimascio.dotenv.Dotenv
import org.apache.kafka.clients.admin.NewTopic
import java.io.FileInputStream
import java.util.Properties

object StreamsUtils {
  private const val PROPERTIES_FILE_PATH = "src/main/resources/streams.properties"
  private const val REPLICATION_FACTOR: Short = 3
  private const val PARTITIONS = 6

  private val dotenv = Dotenv.load()

  fun loadProperties() =
    Properties().apply {
      FileInputStream(PROPERTIES_FILE_PATH).use { load(it) }
      setProperty("bootstrap.servers", dotenv["BOOTSTRAP_SERVERS"])
      setProperty(
        "sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule " +
            "required username=\"${dotenv["KAFKA_USER"]}\" password=\"${dotenv["KAFKA_PASSWORD"]}\";"
      )
      setProperty("value.converter.schema.registry.url", dotenv["SCHEMA_REGISTRY_URL"])
      setProperty("value.converter.schema.registry.basic.auth.user.info",
        "${dotenv["SCHEMA_REGISTRY_USER"]}:${dotenv["SCHEMA_REGISTRY_PASSWORD"]}")
    }

  fun createTopic(topicName: String) = NewTopic(topicName, PARTITIONS, REPLICATION_FACTOR)
}
