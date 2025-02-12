package com.github.dhslrl321.kafkastreams

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.kafka.annotation.EnableKafkaStreams

@EnableKafkaStreams
@SpringBootApplication
class KafkaStreamsApplication

fun main(args: Array<String>) {
  runApplication<KafkaStreamsApplication>(*args)
}
