package com.github.dhslrl321.kafkastreams

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaStreamsApplication

fun main(args: Array<String>) {
    runApplication<KafkaStreamsApplication>(*args)
}
