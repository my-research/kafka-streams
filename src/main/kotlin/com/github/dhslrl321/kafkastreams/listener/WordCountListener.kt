package com.github.dhslrl321.kafkastreams.listener

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class WordCountListener {

  private val logger = LoggerFactory.getLogger(WordCountListener::class.java)

  @KafkaListener(topics = ["wordcount-output"], groupId = "wordcount-listener-group")
  fun consumeWordCount(record: ConsumerRecord<String, Long>) {
    val key = record.key()
    val value = record.value()
    logger.info("Consumed record from wordcount-output - Key: $key, Value: $value")
  }
}
