package ch.scigility.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.IOException;
/**
 * Hello world!
 *
 */
public class StreamProducer {
    public static void main( ) throws IOException
    {
        //Assign topicName to string Variable
        String topicName = "other_topic";
        //create instance for properties to access producer configs
        Properties props = new Properties();
        // assign localhost id
        props.put("bootstrap.servers", "pocathon-confluent1:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String,String>(props);
        for(int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<String,String>(topicName, Integer.toString(i), Integer.toString(i)));
        System.out.println("Message sent successfully");
        producer.close();
    }
}
