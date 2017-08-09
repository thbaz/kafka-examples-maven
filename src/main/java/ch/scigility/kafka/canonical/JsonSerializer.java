package ch.scigility.kafka.canonical;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSerializer<T> implements Serializer<T> {
	 private final ObjectMapper objectMapper = new ObjectMapper();

	public void close() {
		// TODO Auto-generated method stub

	}

	public void configure(Map<String, ?> arg0, boolean arg1) {
		// TODO Auto-generated method stub

	}

	public byte[] serialize(String topic, T data) {
		 if (data == null)
	            return null;

	        try {
	            return objectMapper.writeValueAsBytes(data);
	        } catch (Exception e) {
	            throw new SerializationException("Error serializing JSON message", e);
	        }
	}

}
