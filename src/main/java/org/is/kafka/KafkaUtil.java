package org.is.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

public class KafkaUtil {

  public static final String BOOTSTRAP_SERVERS = "localhost:29092";
  public static final String CUSTOMER_SALE_TOPIC = "customer.sale";

  private static final Random RANDOM = new Random();
  private static final int MAX = 100;

  private static final Map<String, String> filesMap = new ConcurrentHashMap<>();

  private static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder()
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .build();

  public static Properties getConsumerProperties() {
    var properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer_" + UUID.randomUUID());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return properties;
  }

  public static Properties getProducerProperties() {
    var properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return properties;
  }

  public static Properties getStreamProperties() {
    var properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream_" + UUID.randomUUID());
    return properties;
  }

  public static <T> T parseTablePayload(String json, Class<T> type) {
    var payloadStr = JsonPath.read(json, "$.payload.after");
    return OBJECT_MAPPER.convertValue(payloadStr, type);
  }

  public static <T> T readValue(String json, Class<T> type) {
    try {
      return OBJECT_MAPPER.readValue(json, type);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static String writeValue(Object object) {
    try {
      return OBJECT_MAPPER.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static String readFileAsString(String fileName) {
    return filesMap.computeIfAbsent(fileName, (key) -> {
      var is = KafkaUtil.class.getClassLoader().getResourceAsStream(fileName);
      try {
        return IOUtils.toString(is, StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  public static int randomInt() {
    return randomInt(MAX);
  }

  public static int randomInt(int max) {
    return RANDOM.nextInt(max);
  }

  public static Double randomDouble() {
    return randomDouble(1);
  }

  public static Double randomDouble(int min) {
    return ThreadLocalRandom.current().nextDouble(min, min + (double) MAX);
  }
}
