package org.is.kafka;

import static org.is.kafka.KafkaUtil.getConsumerProperties;
import static org.is.kafka.KafkaUtil.getProducerProperties;
import static org.is.kafka.KafkaUtil.parseTablePayload;
import static org.is.kafka.KafkaUtil.randomInt;
import static org.is.kafka.KafkaUtil.readFileAsString;
import static org.is.kafka.KafkaUtil.writeValue;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.is.kafka.model.Purchase;
import org.is.kafka.model.Sock;
import org.is.kafka.model.Sock.SockType;
import org.is.kafka.model.Supplier;

public class PurchaseOrdersApp {

  private static final int COUNT = 2;

  private static final KafkaConsumer<String, String> CONSUMER = new KafkaConsumer<>(getConsumerProperties());
  private static final KafkaProducer<String, String> PRODUCER = new KafkaProducer<>(getProducerProperties());

  public static void main(String[] args) {
    CONSUMER.subscribe(List.of("sock-shop.public.supplier"));

    while (true) {
      var records = CONSUMER.poll(Duration.ofMillis(100));

      for (var rec : records) {
        var supplier = parseTablePayload(rec.value(), Supplier.class);
        System.out.println("Read supplier from db: " + supplier.name());
        // for every supplier generate 100 random socks
        for (int i = 0; i < COUNT; i++) {
          var sock = generateAndSendSock(supplier.id());
          generateAndSendPurchase(sock);
        }
      }
    }
  }

  private static Sock generateAndSendSock(UUID supplierId) {
    var sock = Sock.builder()
        .id(UUID.randomUUID())
        .type(SockType.random())
        .price(KafkaUtil.randomDouble())
        .supplierId(supplierId)
        .build();

    var sockJson = readFileAsString("sock.json").formatted(writeValue(sock));
    var producerRecord = new ProducerRecord<String, String>("sock", sockJson);
    PRODUCER.send(producerRecord);

    return sock;
  }

  private static void generateAndSendPurchase(Sock sock) {
    var purchase = Purchase.builder()
        .id(UUID.randomUUID())
        .sockId(sock.id())
        .quantity(randomInt())
        .sock(sock)
        .build();

    var purchaseJson = readFileAsString("purchase.json").formatted(writeValue(purchase));
    var producerRecord = new ProducerRecord<String, String>("purchase", purchaseJson);
    PRODUCER.send(producerRecord);
  }
}
