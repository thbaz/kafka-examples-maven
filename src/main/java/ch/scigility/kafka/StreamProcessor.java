package ch.scigility.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import java.io.IOException;

import java.util.Arrays;
import java.util.Properties;

public class StreamProcessor {

  public static void main() throws IOException {

    Properties props = new Properties();
    // assign localhost id
    props.put("application.id", "ch.scigility.kafka.stream");
    props.put("bootstrap.servers", "172.31.24.135:9092");
    props.put("acks", "all");
    props.put("retries", 5);
    props.put("batch.size", 16384);
    props.put("linger.ms", 100);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    KStreamBuilder builder = new KStreamBuilder();

    final Serde<String> stringSerde = Serdes.String();
    final Serde<byte[]> byteArraySerde = Serdes.ByteArray();
    final KStream<byte[], String> textLines = builder.stream(byteArraySerde, stringSerde, "co_full_1");
    final KStream<byte[], String> uppercasedWithMapValues = textLines.mapValues(String::toUpperCase);
    uppercasedWithMapValues.to("UppercasedTextLinesTopic");


    System.out.println("fullStream");
    KStream<String, String> fullStream = builder.stream("co_full_out");
    fullStream.print();

    System.out.println("cdcStream");
    KStream<String, String> cdcStream = builder.stream("co_cdc_out");
    cdcStream.print();

    //KTable<String, Long> wordCounts = textLines.countByKey("fieldId");
    //wordCounts.to("another_topic");
    System.out.println("KafkaStreams");
    KafkaStreams streams = new KafkaStreams(builder, props);
    streams.start();
  }

}
