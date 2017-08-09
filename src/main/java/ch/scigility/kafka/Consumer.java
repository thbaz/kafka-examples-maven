package ch.scigility.kafka;

import java.net.URLEncoder;
import java.util.UUID;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.openx.data.jsonserde.json.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;

public class Consumer {

	public static void main() throws IOException {
        // set up house-keeping
        Properties props = new Properties();
        props.put("zookeeper.connect", "127.0.0.1:2181");
        props.put("bootstrap.servers", "172.31.24.135:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "your_client_id");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // and the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("co_cdc_1"));

        //offset = 0, key = null, value = DBS-H_TESToffset = 1, key = null, value = {
        // "commandScn": "1195604",
        // "commandCommitScn": "1195604",
        // "commandSequence": "0",
        // "commandType": "INSERT",
        // "commandTimestamp": "2017-08-08 09:36:43+00:000",
        // "objectDBName": "DB1",
        // "objectSchemaName": "POC1",
        // "objectId": "CORE_CONTRACTS",
        // "changedFieldsList": [
        //    {
        //        "fieldId": "COCO_ID",
        //        "fieldType": "NUMBER",
        //        "fieldValue": "1",
        //        "fieldChanged": "Y"
        //    },
        //    {
        //        "fieldId": "COCO_TYPE",
        //        "fieldType": "NUMBER",
        //        "fieldValue": "2",
        //        "fieldChanged": "Y"
        //    },
        //  ]
        // }


        while (true) {
         ConsumerRecords<String, String> records = consumer.poll(100);
         for (ConsumerRecord<String, String> record : records){
           System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
           //Container container = new Gson().fromJson(URLEncoder.encode(record.value(), "UTF-8"), Container.class);
         }

        }
    }
}
