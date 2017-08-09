package ch.scigility.kafka;

import org.apache.kafka.common.serialization.Serdes;
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
    props.put("bootstrap.servers", "pocathon-confluent1:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    KStreamBuilder builder = new KStreamBuilder();
    KStream<String, String> inputStream = builder.stream("co_full_1");
    inputStream.print();

    //KTable<String, Long> wordCounts = textLines.countByKey("fieldId");
    //wordCounts.to("another_topic");

    KafkaStreams streams = new KafkaStreams(builder, props);
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

}
