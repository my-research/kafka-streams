package com.github.dhslrl321.kafkastreams.streams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class ToUpperCaseStreamsConfig {
  @Bean
  fun kStream(builder: StreamsBuilder): KStream<String, String> {
    val inputTopic = "my-topic"
    val outputTopic = "my-topic-output"

    val stream: KStream<String, String> = builder.stream(
      inputTopic, Consumed.with(Serdes.String(), Serdes.String())
    )

    stream.mapValues { value ->
        value.uppercase()
      }.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()))

    return stream
  }
}



