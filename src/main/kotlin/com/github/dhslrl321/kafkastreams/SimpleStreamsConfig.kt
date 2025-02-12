package com.github.dhslrl321.kafkastreams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class SimpleStreamsConfig {
    @Bean
    fun kStream(builder: StreamsBuilder): KStream<String, String> {
        val inputTopic = "my-topic"
        val outputTopic = "my-topic-output"

        // "my-topic" 토픽에서 문자열 키와 값으로 스트림 생성
        val stream: KStream<String, String> = builder.stream(
            inputTopic,
            Consumed.with(Serdes.String(), Serdes.String())
        )

        stream
            // 메시지가 전부 소문자인 경우 대문자로 변환
            .mapValues { value ->
                value.uppercase()
            }
            // 결과를 outputTopic 으로 전송
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()))

        return stream
    }
}



