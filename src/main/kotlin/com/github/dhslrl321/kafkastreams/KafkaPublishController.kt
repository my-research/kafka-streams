package com.github.dhslrl321.kafkastreams

import org.springframework.http.ResponseEntity
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

data class PublishRequest(
    val topicName: String,
    val message: String
)

@RestController
class KafkaPublisherController(
    private val kafkaTemplate: KafkaTemplate<String, String>
) {

    @PostMapping("/publish")
    fun publishMessage(@RequestBody request: PublishRequest): ResponseEntity<String> {
        kafkaTemplate.send(request.topicName, request.message)
        return ResponseEntity.ok("메시지가 토픽 [${request.topicName}] 에 전송되었습니다.")
    }
}