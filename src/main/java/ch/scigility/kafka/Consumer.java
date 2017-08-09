package ch.scigility.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.openx.data.jsonserde.json.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;

public class Consumer {

	public static void main() throws IOException {
        // set up house-keeping
        Properties props = new Properties();
        props.put("zookeeper.connect", "127.0.0.1:2181");
        //props.put("bootstrap.servers", "172.31.24.135:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // and the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);


        consumer.subscribe(Arrays.asList("co_full_1"));

        while (true) {
         ConsumerRecords<String, String> records = consumer.poll(100);
         for (ConsumerRecord<String, String> record : records)
             System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
           }
    }


}
