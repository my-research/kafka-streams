package com.github.dhslrl321.kafkastreams

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import java.time.Duration
import java.util.*


@SpringBootTest
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = ["listeners=PLAINTEXT://localhost:9092"]
)
class KafkaStreamsApplicationTests {

    var producer: KafkaProducer<String, String> = KafkaProducer(
        java.util.Map.of<String, Any>(
            "bootstrap.servers", "localhost:9092",
            "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
            "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"
        )
    )
    var consumer: KafkaConsumer<String, String> = KafkaConsumer(
        java.util.Map.of<String, Any>(
            "bootstrap.servers", "localhost:9092",
            "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
        )
    )

    @BeforeEach
    fun setUp() {
        producer.send(ProducerRecord("raw-data", "hi~"))
        producer.send(ProducerRecord("formatted-data", "hi~"))
        producer.flush()
    }

    @Test
    fun sample() {
        // message produce to broker

        val streamsConfiguration = Properties()
        streamsConfiguration[StreamsConfig.APPLICATION_ID_CONFIG] = "map-function-lambda-example"
        streamsConfiguration[StreamsConfig.CLIENT_ID_CONFIG] = "map-function-lambda-example-client"

        // Where to find Kafka broker(s).
        streamsConfiguration[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"

        // Specify default (de)serializers for record keys and for record values.
        val stringSerde = Serdes.String()
        streamsConfiguration[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = stringSerde.javaClass.name
        streamsConfiguration[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = stringSerde.javaClass.name

        val streamsBuilder = StreamsBuilder()

        streamsBuilder
            .stream("raw-data", Consumed.with(stringSerde, stringSerde))
            .mapValues(String::uppercase)
            .to("formatted-data")

        val streams = KafkaStreams(streamsBuilder.build(), streamsConfiguration)

        streams.start()

        consumer.subscribe(listOf("formatted-data"))

        val poll = consumer.poll(Duration.ofSeconds(2))

        assertEquals(2, poll.count(), "formatted-data 토픽에서 2개의 메시지를 수신해야 합니다.")

        // 메시지 내용 검증 (대문자로 변환되었는지 확인)
        val receivedMessages = poll.map { it.value() }
        assertEquals(listOf("hi~", "HI~"), receivedMessages.sorted())

        Runtime.getRuntime().addShutdownHook(Thread { streams.close() })
    }
}
