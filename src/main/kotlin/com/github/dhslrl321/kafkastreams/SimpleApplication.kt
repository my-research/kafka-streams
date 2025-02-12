package com.github.dhslrl321.kafkastreams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import java.util.*

fun main(args: Array<String>) {
  val bootstrapServers: String = if (args.isNotEmpty()) args[0] else "localhost:9092"
  val streamsConfiguration = Properties()


  // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
  // against which the application is run.
  streamsConfiguration[StreamsConfig.APPLICATION_ID_CONFIG] = "map-function-lambda-example"
  streamsConfiguration[StreamsConfig.CLIENT_ID_CONFIG] = "map-function-lambda-example-client"

  // Where to find Kafka broker(s).
  streamsConfiguration[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers

  // Specify default (de)serializers for record keys and for record values.
  streamsConfiguration[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.ByteArray().javaClass.name
  streamsConfiguration[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name

  val stringSerde = Serdes.String()
  val byteArraySerde = Serdes.ByteArray()


  // In the subsequent lines we define the processing topology of the Streams application.
  val builder = StreamsBuilder()


  // Read the input Kafka topic into a KStream instance.
  val textLines = builder.stream("TextLinesTopic", Consumed.with(byteArraySerde, stringSerde))

  // Variant 1: using `mapValues`
  val uppercasedWithMapValues = textLines.mapValues { v: String -> v.uppercase(Locale.getDefault()) }


  // Write (i.e. persist) the results to a new Kafka topic called "UppercasedTextLinesTopic".
  //
  // In this case we can rely on the default serializers for keys and values because their data
  // types did not change, i.e. we only need to provide the name of the output topic.
  uppercasedWithMapValues.to("UppercasedTextLinesTopic")


  // Variant 2: using `map`, modify value only (equivalent to variant 1)
  val uppercasedWithMap =
    textLines.map<ByteArray, String> { key: ByteArray?, value: String ->
      KeyValue(
        key,
        value.uppercase(Locale.getDefault())
      )
    }


  // Variant 3: using `map`, modify both key and value
  //
  // Note: Whether, in general, you should follow this artificial example and store the original
  //       value in the key field is debatable and depends on your use case.  If in doubt, don't
  //       do it.
  val originalAndUppercased = textLines.map<String, String> { key: ByteArray?, value: String ->
    KeyValue.pair(
      value,
      value.uppercase(Locale.getDefault())
    )
  }


  // Write the results to a new Kafka topic "OriginalAndUppercasedTopic".
  //
  // In this case we must explicitly set the correct serializers because the default serializers
  // (cf. streaming configuration) do not match the type of this particular KStream instance.
  originalAndUppercased.to("OriginalAndUppercasedTopic", Produced.with(stringSerde, stringSerde))

  val streams = KafkaStreams(builder.build(), streamsConfiguration)


  // Always (and unconditionally) clean local state prior to starting the processing topology.
  // We opt for this unconditional call here because this will make it easier for you to play around with the example
  // when resetting the application for doing a re-run (via the Application Reset Tool,
  // https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html).
  //
  // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
  // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
  // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
  // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
  // See `ApplicationResetExample.java` for a production-like example.
  streams.cleanUp()
  streams.start()


  // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
  Runtime.getRuntime().addShutdownHook(Thread { streams.close() })
}
