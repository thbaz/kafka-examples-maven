package ch.scigility.kafka.canonical;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
public class JsonDeserializer<T> implements Deserializer<T> {

	    private ObjectMapper objectMapper = new ObjectMapper();

	    private Class<T> tClass;

	    /**
	     * Default constructor needed by Kafka
	     */
	    public JsonDeserializer() {
	    }

		public void close() {
			// TODO Auto-generated method stub

		}

	    @SuppressWarnings("unchecked")
		public void configure(Map<String, ?> props, boolean isKey) {
	        tClass = (Class<T>) props.get("JsonPoJOClass");
	    }


	    public T deserialize(String topic, byte[] bytes) {
	        if (bytes == null)
	            return null;

	        T data;
	        try {
	            data = objectMapper.readValue(bytes, tClass);
	        } catch (Exception e) {
	            throw new SerializationException(e);
	        }

	        return data;
	    }


}
