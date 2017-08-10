package ch.scigility.kafka;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import java.net.URLEncoder;
import java.util.UUID;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.common.errors.SerializationException;
import ch.scigility.kafka.canonical.JsonDeserializer;
import ch.scigility.kafka.canonical.JsonSerializer;
import ch.scigility.kafka.canonical.Landing;
import ch.scigility.kafka.canonical.Partners;
import ch.scigility.kafka.canonical.avro.ContractsSchema;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import org.openx.data.jsonserde.json.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import java.util.Map;
import java.util.Properties;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.kafka.clients.producer.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;

import ch.scigility.kafka.canonical.ChangedFieldsList;

import org.apache.kafka.common.serialization.StringSerializer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

public class Consumer {

	public static void main(String[] args) throws IOException {

		//String schema = String(Files.readAllBytes(Paths.get("resources/contract_schema.avsc"),StandardCharsets.UTF_8));
		System.out.println("Consumer procedure");
		//Schema schema = new Schema.Parser().parse(new File("resources/ContractSchema.avsc"));

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
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("co_full_1"));
		System.out.println("KafkaConsumer Configured");

		Schema schema = ContractsSchema.SCHEMA$;
		Properties propsAvro = new Properties();
		propsAvro.put("zookeeper.connect", "127.0.0.1:2181");
		propsAvro.put("bootstrap.servers", "172.31.24.135:9092");
		propsAvro.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
			io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		propsAvro.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
			io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		propsAvro.put("schema.registry.url", "127.0.0.1:8081");
		KafkaProducer producer = new KafkaProducer(propsAvro);
		System.out.println("KafkaProducer Configured");

		while (true) {

			ConsumerRecords<String, String> records = consumer.poll(100);

			for (ConsumerRecord<String, String> record : records){
				System.out.println("RECORD BEGIN:");
				System.out.printf(record.value());
				try {
					System.out.println("Deserialize:");
					Landing landing = new ObjectMapper().readValue(record.value(), Landing.class);
					System.out.println(landing.toString());

					if(landing.getObjectId().equals("CORE_CONTRACTS")){
						System.out.println("Threat Contracts:");
						ContractsSchema avroRecord = landing.toAvroCanonical();
						System.out.println(avroRecord.toString());

						ProducerRecord<Object, Object> prodAvroRecord = new ProducerRecord<>(
							"co_full_contracts", "avrokey", avroRecord
						);

						try {
							System.out.println("Sending...");
						  producer.send(prodAvroRecord);
							System.out.println("Sent");
						} catch(SerializationException e) {
						  System.out.println("SerializationException");
							e.printStackTrace();
						}
					}
					System.out.println("RECORD END...");
				}
				catch (IOException e) {
					System.out.printf("IOException");
					e.printStackTrace();
				}
				System.out.println("...");
			}
		}
	}
}
