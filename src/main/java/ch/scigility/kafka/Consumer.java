package ch.scigility.kafka;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import java.net.URLEncoder;
import java.util.UUID;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import ch.scigility.kafka.canonical.JsonDeserializer;
import ch.scigility.kafka.canonical.JsonSerializer;
import ch.scigility.kafka.canonical.Landing;
import ch.scigility.kafka.canonical.Partners;
import ch.scigility.kafka.canonical.ContractSchema;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import org.openx.data.jsonserde.json.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.kafka.clients.producer.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ch.scigility.kafka.canonical.ChangedFieldsList;

import org.apache.kafka.common.serialization.StringSerializer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

public class Consumer {

	public static void main() throws IOException {

        String schema = String(Files.readAllBytes(Paths.get("resources/contract_schema.avsc"),StandardCharsets.UTF_8));
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

        Properties propsAvro = new Properties();
                // hardcoding the Kafka server URI for this example
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", schema);


        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // and the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("co_full_1"));

        String value = "{ \n"+
        "\"commandScn\": \"1195604\",\n"+
        "\"commandCommitScn\": \"1195604\",\n"+
        "\"commandSequence\": \"0\",\n"+
        "\"commandType\": \"INSERT\",\n"+
        "\"commandTimestamp\": \"2017-08-08 09:36:43+00:000\",\n"+
        "\"objectDBName\": \"DB1\",\n"+
        "\"objectSchemaName\": \"POC1\",\n"+
        "\"objectId\": \"CORE_CONTRACTS\",\n"+
        "\"changedFieldsList\": [\n"+
        "   {\n"+
        "       \"fieldId\": \"COCO_ID\",\n"+
        "       \"fieldType\": \"NUMBER\",\n"+
        "       \"fieldValue\": \"1\",\n"+
        "       \"fieldChanged\": \"Y\"\n"+
        "     },\n"+
        "   {\n"+
        "       \"fieldId\": \"COCO_TYPE\",\n"+
        "       \"fieldType\": \"NUMBER\",\n"+
        "       \"fieldValue\": \"2\",\n"+
        "     \"fieldChanged\": \"Y\"\n"+
        "   }\n"+
        "    ]\n"+
        "   }";
        //SERDE Landing
				System.out.println("SERDE Landing");
				Map<String, Object> landingProps = new HashMap<>();

				final Serializer<Landing> landingSerializer = new JsonSerializer<>();
				landingProps.put("JsonPOJOClass", Landing.class);
				landingSerializer.configure(landingProps, false);

				final Deserializer<Landing> landingDeserializer = new JsonDeserializer<>();
				landingProps.put("JsonPOJOClass", Landing.class);
				landingDeserializer.configure(landingProps, false);

				final Serde<Landing> LandingSerde = Serdes.serdeFrom(landingSerializer, landingDeserializer);

        //SERDE Partner
        System.out.println("SERDE Partner");
        Map<String, Object> partnerProps = new HashMap<>();

        final Serializer<Landing> partnerSerializer = new JsonSerializer<>();
        partnerProps.put("JsonPOJOClass", Partners.class);
        partnerSerializer.configure(partnerProps, false);

        final Deserializer<Landing> partnerDeserializer = new JsonDeserializer<>();
        partnerProps.put("JsonPOJOClass", Partners.class);
        partnerDeserializer.configure(partnerProps, false);

        final Serde<Landing> PartnerSerde = Serdes.serdeFrom(partnerSerializer, partnerDeserializer);

        // System.out.println("Builder");
        // System.out.println(value);
        //
        // System.out.println("ObjectMapper");
        // Landing landing = new ObjectMapper().readValue(value, Landing.class);
        // System.out.println(landing.getCommandScn());
        // System.out.println(landing.getCommandCommitScn());
        // System.out.println(landing.toString());

        //Landing landing = new Gson().fromJson(URLEncoder.encode(value, "UTF-8"), Landing.class);
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        while (true) {

         ConsumerRecords<String, String> records = consumer.poll(100);

         for (ConsumerRecord<String, String> record : records){

           //Partners partners = new Gson().fromJson(URLEncoder.encode(record.value(), "UTF-8"), Partners.class);
           //Partners partners = new Gson().fromJson(URLEncoder.encode(value, "UTF-8"), Partners.class);
           try {

           Landing landing = new ObjectMapper().readValue(record.value(), Landing.class);
           //Container container = new Gson().fromJson(URLEncoder.encode(record.value(), "UTF-8"), Container.class);

           if(landing.getObjectId().equals("CORE_CONTRACTS")){
             System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
             System.out.println(landing.toString());

             //for all changedFieldsList
             List<ChangedFieldsList> changedFieldsList = landing.getChangedFieldsList();
             for (int i = 0; i < changedFieldsList.size(); i++) {
			            System.out.println(changedFieldsList.get(i).getFieldId());
                  System.out.println(changedFieldsList.get(i).getFieldValue());
                  //getFieldId
                  //getFieldValue
                  if( changedFieldsList.get(i).getFieldId().equals("COCO_ID") ){
                    producer.send(new ProducerRecord<String,String>("co_full_out", "INCO_ID", changedFieldsList.get(i).getFieldValue()));
                    ProducerRecord<String, contract_schema> record = new ProducerRecord<String, contract_schema>("co_full_contracts", event.getIp().toString(), event);
                    producer.send(record).get();

                  }
              }
            }
          }
       catch (IOException ex) {
         System.out.printf("IOException");
       }


          }
        }
    }
}
