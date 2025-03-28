plugins {
    kotlin("jvm") version "2.1.10"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.0.0"
}

group = "org.adrianliz"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven")
    }
}

dependencies {
    implementation("io.github.cdimascio:dotenv-kotlin:6.2.2")
    implementation("org.apache.kafka:kafka-streams:3.5.1") {
        exclude(group = "org.apache.kafka", module = "kafka-clients")
    }
    implementation("org.apache.kafka:kafka-clients:3.5.1!!")
    implementation("org.apache.avro:avro:1.10.2")
    implementation("io.confluent:kafka-streams-avro-serde:6.1.1") {
        exclude(group = "org.apache.kafka", module = "kafka-clients")
        exclude(group = "org.apache.kafka", module = "kafka-streams")
    }

    implementation("org.apache.logging.log4j:log4j-api:2.17.2")
    implementation("org.apache.logging.log4j:log4j-core:2.17.2")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.17.2")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}
