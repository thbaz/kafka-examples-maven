package ch.scigility.kafka;

import java.util.UUID;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.io.IOException;

import java.util.Arrays;
import java.util.Properties;

public class StreamProcessor {

  public static void main() throws IOException {

    Properties props = new Properties();
    // assign localhost id
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ch.scigility.kafka.stream");
    props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "127.0.0.1:2181");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.31.24.135:9092");
    props.put(StreamsConfig.CLIENT_ID_CONFIG, "generali");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    KStreamBuilder builder = new KStreamBuilder();

    final Serde<String> stringSerde = Serdes.String();
    final Serde<byte[]> byteArraySerde = Serdes.ByteArray();
    final KStream<byte[], String> textLines = builder.stream(byteArraySerde, stringSerde, "co_full_1");
    final KStream<byte[], String> uppercasedWithMapValues = textLines.mapValues(String::toUpperCase);
    uppercasedWithMapValues.to("UppercasedTextLinesTopic");

    System.out.println("fullStream");
    KStream<String, String> fullStream = builder.stream("co_full_out");
    fullStream.print();

    //KTable<String, Long> wordCounts = textLines.countByKey("fieldId");
    //wordCounts.to("another_topic");
    System.out.println("KafkaStreams");
    KafkaStreams streams = new KafkaStreams(builder, props);
    streams.start();
  }
}
