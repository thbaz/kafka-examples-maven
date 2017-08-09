package ch.scigility.kafka.canonical;

import java.io.IOException;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import ch.scigility.kafka.canonical.JsonDeserializer;
import ch.scigility.kafka.canonical.JsonSerializer;
import ch.scigility.kafka.canonical.Landing;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Partners {
	private static boolean isPartners(Landing raw) {
		if (raw.getObjectId().equals("CORE_CONTRACTS") || raw.getObjectId().equals("CORE_AGENTS")) {
			return true;
		}
		return false;
	}

	 public static void main() throws IOException {
		   String landingTopic = "co_cdc_1";
		 		final String bootstrapServers = "172.31.24.135:9092";
		    final Properties streamsConfiguration = new Properties();
		    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "canonical-partners");
		    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "canonical-partners-client");
		    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

		    //SERDE Landing
	        Map<String, Object> landingProps = new HashMap<>();

	        final Serializer<Landing> landingSerializer = new JsonSerializer<>();
	        landingProps.put("JsonPOJOClass", Landing.class);
	        landingSerializer.configure(landingProps, false);

	        final Deserializer<Landing> landingDeserializer = new JsonDeserializer<>();
	        landingProps.put("JsonPOJOClass", Landing.class);
	        landingDeserializer.configure(landingProps, false);

	        final Serde<Landing> LandingSerde = Serdes.serdeFrom(landingSerializer, landingDeserializer);


	        //SERDE Partner
	        Map<String, Object> partnerProps = new HashMap<>();

	        final Serializer<Landing> partnerSerializer = new JsonSerializer<>();
	        partnerProps.put("JsonPOJOClass", Partners.class);
	        partnerSerializer.configure(partnerProps, false);

	        final Deserializer<Landing> partnerDeserializer = new JsonDeserializer<>();
	        partnerProps.put("JsonPOJOClass", Partners.class);
	        partnerDeserializer.configure(partnerProps, false);

	        final Serde<Landing> PartnerSerde = Serdes.serdeFrom(partnerSerializer, partnerDeserializer);


		    final KStreamBuilder builder = new KStreamBuilder();


		    builder.stream(Serdes.String(), LandingSerde, landingTopic)
		    		.filter((key, raw) -> isPartners(raw))
		    		// map to PArtner .map((key, partner) -> new KeyValue<>());
		    		// .to new topic
		    ;



		    final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
		    streams.cleanUp();
		    streams.start();

		    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
		    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	 }
}
