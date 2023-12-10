package org.is.kafka;

import static org.is.kafka.KafkaUtil.CUSTOMER_SALE_TOPIC;
import static org.is.kafka.KafkaUtil.getConsumerProperties;
import static org.is.kafka.KafkaUtil.getProducerProperties;
import static org.is.kafka.KafkaUtil.parseTablePayload;
import static org.is.kafka.KafkaUtil.randomDouble;
import static org.is.kafka.KafkaUtil.randomInt;
import static org.is.kafka.KafkaUtil.writeValue;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.is.kafka.model.Sale;
import org.is.kafka.model.Sock;

public class CustomersApp {

  private static final int COUNT = 2;
  private static final KafkaConsumer<String, String> CONSUMER = new KafkaConsumer<>(getConsumerProperties());
  private static final KafkaProducer<String, String> PRODUCER = new KafkaProducer<>(getProducerProperties());
  private static final List<String> CUSTOMER_NAMES = List.of(
      "Michael Jackson",
      "Barack Obama",
      "Michael Jordan",
      "Cristiano Ronaldo",
      "Tom Hanks",
      "Kobe Bryant"
  );


  public static void main(String[] args) {
    CONSUMER.subscribe(List.of("sock-shop.public.sock"));

    while (true) {
      var records = CONSUMER.poll(Duration.ofMillis(100));

      for (var rec : records) {
        var sock = parseTablePayload(rec.value(), Sock.class);
        generateAndSendSales(sock);
      }
    }
  }

  private static void generateAndSendSales(Sock sock) {
    for (int i = 0; i < COUNT; i++) {
      var sale = Sale.builder()
          .id(UUID.randomUUID())
          .sock(sock)
          .customerName(CUSTOMER_NAMES.get(randomInt(CUSTOMER_NAMES.size())))
          .pricePerPair(randomDouble(sock.price().intValue()))
          .quantity(randomInt())
          .build();

      var producerRecord = new ProducerRecord<String, String>(CUSTOMER_SALE_TOPIC, writeValue(sale));
      PRODUCER.send(producerRecord);
    }
  }
}
