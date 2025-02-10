package com.github.dhslrl321.kafkastreams

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka


@SpringBootTest
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = ["listeners=PLAINTEXT://localhost:9092"]
)
class KafkaStreamsApplicationTests {

    private val props: Map<String, Any> = java.util.Map.of<String, Any>(
        "bootstrap.servers", "localhost:9092",
        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
        "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"
    )

    var producer: KafkaProducer<String, String> = KafkaProducer(props)

    @Test
    fun sample() {
        // message produce to broker
        val record = ProducerRecord<String, String>("a-topic", "hi~")

        producer.send(record)
    }
}
