package com.github.dhslrl321.kafkastreams.streams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class WordCountStreamsConfig {
  @Bean
  fun wordCountStream(builder: StreamsBuilder): KStream<String, Long> {
    val inputTopic = "wordcount-input"
    val outputTopic = "wordcount-output"

    val textLines: KStream<String, String> = builder.stream(
      inputTopic,
      Consumed.with(Serdes.String(), Serdes.String())
    )

    val wordCounts = textLines
      .flatMapValues { line -> line.lowercase().split("\\W+".toRegex()) }
      .groupBy({ _, word -> word }, Grouped.with(Serdes.String(), Serdes.String()))
      .count()

    wordCounts.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()))

    return wordCounts.toStream()
  }
}