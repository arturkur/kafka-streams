package org.is.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.is.kafka.KafkaUtil.CUSTOMER_SALE_TOPIC;
import static org.is.kafka.KafkaUtil.getStreamProperties;
import static org.is.kafka.KafkaUtil.parseTablePayload;
import static org.is.kafka.KafkaUtil.readFileAsString;
import static org.is.kafka.KafkaUtil.readValue;
import static org.is.kafka.KafkaUtil.writeValue;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.UnaryOperator;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.is.kafka.model.Result;
import org.is.kafka.model.Sale;
import org.is.kafka.model.Supplier;

public class KafkaStreamsApp {

  private static final StreamsBuilder BUILDER = new StreamsBuilder();
  private static final KStream<String, String> SALES_STREAM = BUILDER.stream(CUSTOMER_SALE_TOPIC);

  private static final ToDoubleFunction<Sale> REVENUE_FUNCTION = sale -> sale.pricePerPair() * sale.quantity();
  private static final ToDoubleFunction<Sale> EXPENSE_FUNCTION = sale -> sale.sock().price() * sale.quantity();
  private static final ToDoubleFunction<Sale> PROFIT_FUNCTION = sale -> {
    var revenue = sale.pricePerPair() * sale.quantity();
    var expenses = sale.sock().price() * sale.quantity();
    return revenue - expenses;
  };

  private static final Serde<CountAndSum> COUNT_AND_SUM_SERDE = Serdes.serdeFrom(new CountAndSumSerializer(), new CountAndSumDeserializer());
  private static final Serde<ProfitPerType> PROFIT_PER_TYPE_SERDE = Serdes.serdeFrom(new ProfitPerTypeSerializer(), new ProfitPerTypeDeserializer());

  public static void main(String[] args) {
    requirement2();
    requirement5();
    requirement6();
    requirement7();
    requirement8();
    requirement9();
    requirement10();
    requirement11();
    requirement12();
    requirement13();
    requirement14();
    requirement15();
    requirement16();
    requirement17();

    saveSalesToDb();

    var streams = new KafkaStreams(BUILDER.build(), getStreamProperties());
    streams.start();
  }

  private static void requirement2() {
    BUILDER.stream("sock-shop.public.supplier")
        .mapValues(String.class::cast)
        .mapValues(value -> parseTablePayload(value, Supplier.class))
        .mapValues(Supplier::name)
        .map((k, v) -> new KeyValue<>("1", v))
        .groupByKey(Grouped.keySerde(Serdes.String()))
        .reduce((v1, v2) -> v1 + ", " + v2)
        .toStream()
        .mapValues(value -> Result.builder()
            .id("req2")
            .requirement("List sock suppliers")
            .result(value)
            .build())
        .mapValues(KafkaUtil::writeValue)
        .map((key, payload) -> {
          var json = readFileAsString("result.json").formatted(payload);
          return new KeyValue<>(null, json);
        })
        .to("result");
  }

  private static void requirement5() {
    calculatePerSock(
        "req5_sock_id_",
        "Get the revenue per sock pair sale",
        REVENUE_FUNCTION
    );
  }

  private static void requirement6() {
    calculatePerSock(
        "req6_sock_id_",
        "Get the expenses per sock pair sale",
        EXPENSE_FUNCTION
    );
  }

  private static void requirement7() {
    calculatePerSock(
        "req7_sock_id_",
        "Get the profit per sock pair sale",
        PROFIT_FUNCTION
    );
  }

  private static void requirement8() {
    calculateTotal(
        "req8",
        "Get the total revenues",
        REVENUE_FUNCTION
    );
  }

  private static void requirement9() {
    calculateTotal(
        "req9",
        "Get the total expenses",
        EXPENSE_FUNCTION
    );
  }

  private static void requirement10() {
    calculateTotal(
        "req10",
        "Get the total profit",
        PROFIT_FUNCTION
    );
  }

  private static void requirement11() {
    calculateAverage(
        sale -> sale.sock().type().toString(),
        key -> "req11_type_" + key,
        "Get the average amount spent in each purchase (by sock type)"
    );
  }

  private static void requirement12() {
    calculateAverage(
        sale -> "1",
        key -> "req12",
        "Get the average amount spent in each purchase (for all socks)"
    );
  }

  private static void requirement13() {
    SALES_STREAM.mapValues(String.class::cast)
        .mapValues(json -> readValue(json, Sale.class))
        .map((k, v) -> new KeyValue<>(v.sock().type().toString(), PROFIT_FUNCTION.applyAsDouble(v)))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
        .reduce(Double::sum)
        .toStream()
        .map((k, v) -> new KeyValue<>("1", new ProfitPerType(k, v)))
        .groupByKey(Grouped.with(Serdes.String(), PROFIT_PER_TYPE_SERDE))
        .aggregate(
            () -> new ProfitPerType("", 0.0),
            (key, value, aggregate) -> {
              var oldBest = aggregate.profit();
              var newBest = Math.max(oldBest, value.profit());
              return newBest >= oldBest
                  ? value
                  : aggregate;
            },
            Materialized.with(Serdes.String(), PROFIT_PER_TYPE_SERDE)
        )
        .toStream()
        .map((k, v) -> new KeyValue<>(null, Result.builder()
            .id("req13")
            .requirement("Get the sock type with the highest profit")
            .result("type: %s, profit: %s".formatted(v.type(), v.profit()))
            .build()))
        .mapValues(payload -> readFileAsString("result.json")
            .formatted(writeValue(payload)))
        .to("result");
  }

  private static void requirement14() {
    calculateTotalForTimeWindow(
        "req14",
        "Get the total revenue in the last ",
        REVENUE_FUNCTION
    );
  }

  private static void requirement15() {
    calculateTotalForTimeWindow(
        "req15",
        "Get the total expenses in the last ",
        EXPENSE_FUNCTION
    );
  }

  private static void requirement16() {
    calculateTotalForTimeWindow(
        "req16",
        "Get the total profit in the last ",
        PROFIT_FUNCTION
    );
  }

  private static void requirement17() {
    var suppliers = BUILDER.stream("sock-shop.public.supplier")
        .mapValues(String.class::cast)
        .mapValues(json -> parseTablePayload(json, Supplier.class))
        .map((k, v) -> new KeyValue<>(v.id().toString(), v.name()));

    SALES_STREAM.mapValues(String.class::cast)
        .mapValues(json -> readValue(json, Sale.class))
        .map((k, v) -> new KeyValue<>(v.sock().supplierId().toString(), PROFIT_FUNCTION.applyAsDouble(v)))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
        .reduce(Double::sum)
        .toStream()
        .map((k, v) -> new KeyValue<>("1", new ProfitPerType(k, v)))
        .groupByKey(Grouped.with(Serdes.String(), PROFIT_PER_TYPE_SERDE))
        .aggregate(
            () -> new ProfitPerType("", 0.0),
            (key, value, aggregate) -> {
              var oldBest = aggregate.profit();
              var newBest = Math.max(oldBest, value.profit());
              return newBest >= oldBest
                  ? value
                  : aggregate;
            },
            Materialized.with(Serdes.String(), PROFIT_PER_TYPE_SERDE)
        )
        .toStream()
        .map((k, v) -> new KeyValue<>(v.type(), v.profit().toString()))
        .join(
            suppliers,
            (profitPerKey, supplier) -> new ProfitPerType(supplier, Double.valueOf(profitPerKey)),
            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(30))
        )
        .map((k, v) -> new KeyValue<>(null, Result.builder()
            .id("req17")
            .requirement("Get the name of the sock supplier generating the highest profit")
            .result("supplier: %s, profit: %s".formatted(v.type(), v.profit()))
            .build()))
        .mapValues(payload -> readFileAsString("result.json")
            .formatted(writeValue(payload)))
        .to("result");
  }

  private static void calculatePerSock(String resultIdPrefix, String requirement, ToDoubleFunction<Sale> calculationFunction) {
    SALES_STREAM.mapValues(String.class::cast)
        .mapValues(json -> readValue(json, Sale.class))
        .map((k, v) -> new KeyValue<>(v.sock().id().toString(), calculationFunction.applyAsDouble(v)))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
        .reduce(Double::sum)
        .toStream()
        .map((k, v) -> new KeyValue<>(null, Result.builder()
            .id(resultIdPrefix + k)
            .requirement(requirement)
            .result(v.toString())
            .build()))
        .mapValues(payload -> readFileAsString("result.json")
            .formatted(writeValue(payload)))
        .to("result");
  }

  private static void calculateTotal(String resultId, String requirement, ToDoubleFunction<Sale> calculationFunction) {
    SALES_STREAM.mapValues(String.class::cast)
        .mapValues(json -> readValue(json, Sale.class))
        .map((k, v) -> new KeyValue<>("1", calculationFunction.applyAsDouble(v)))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
        .reduce(Double::sum)
        .toStream()
        .map((k, v) -> new KeyValue<>(null, Result.builder()
            .id(resultId)
            .requirement(requirement)
            .result(v.toString())
            .build()))
        .mapValues(payload -> readFileAsString("result.json")
            .formatted(writeValue(payload)))
        .to("result");
  }

  private static void calculateAverage(Function<Sale, String> keyFunction, UnaryOperator<String> resultIdFunction, String requirement) {
    SALES_STREAM.mapValues(String.class::cast)
        .mapValues(json -> readValue(json, Sale.class))
        .map((k, v) -> new KeyValue<>(keyFunction.apply(v), v.sock().price() + v.quantity()))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
        .aggregate(
            () -> new CountAndSum(0, 0.0),
            (key, value, countAndSum) -> {
              countAndSum.setCount(countAndSum.getCount() + 1);
              countAndSum.setSum(countAndSum.getSum() + value);
              return countAndSum;
            },
            Materialized.with(Serdes.String(), COUNT_AND_SUM_SERDE)
        )
        .toStream()
        .map((k, v) -> new KeyValue<>(null, Result.builder()
            .id(resultIdFunction.apply(k))
            .requirement(requirement)
            .result(v.average().toString())
            .build()))
        .mapValues(payload -> readFileAsString("result.json")
            .formatted(writeValue(payload)))
        .to("result");
  }

  private static void calculateTotalForTimeWindow(String resultId, String requirement, ToDoubleFunction<Sale> calculationFunction) {
    var windowDuration = Duration.ofSeconds(15);

    SALES_STREAM.mapValues(String.class::cast)
        .mapValues(json -> readValue(json, Sale.class))
        .map((k, v) -> new KeyValue<>("1", calculationFunction.applyAsDouble(v)))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(windowDuration))
        .reduce(Double::sum)
        .toStream()
        .map((k, v) -> new KeyValue<>(null, Result.builder()
            .id(resultId)
            .requirement(requirement + windowDuration)
            .result(v.toString())
            .build()))
        .mapValues(payload -> readFileAsString("result.json")
            .formatted(writeValue(payload)))
        .to("result");
  }

  private static void saveSalesToDb() {
    @Builder
    record SaleEntity(
       UUID id,
       @JsonProperty("sock_id")
       UUID sockId,
       @JsonProperty("customer_name")
       String customerName,
       @JsonProperty("price_per_pair")
       Double pricePerPair,
       int quantity
    ) {}
    SALES_STREAM.mapValues(String.class::cast)
        .mapValues(json -> readValue(json, Sale.class))
        .mapValues(sale -> new SaleEntity(
            sale.id(),
            sale.sock().id(),
            sale.customerName(),
            sale.pricePerPair(),
            sale.quantity()
        ))
        .mapValues(payload -> readFileAsString("sale.json")
            .formatted(writeValue(payload)))
        .to("sale");
  }

  @Getter
  @Setter
  @NoArgsConstructor
  @AllArgsConstructor
  private static class CountAndSum {
    private int count;
    private double sum;

    public Double average() {
      return sum / count;
    }
  }

  record ProfitPerType(String type, Double profit) {}

  private static class CountAndSumSerializer implements Serializer<CountAndSum> {

    @Override
    public byte[] serialize(String topic, CountAndSum data) {
      return writeValue(data).getBytes(UTF_8);
    }
  }

  private static class CountAndSumDeserializer implements Deserializer<CountAndSum> {

    @Override
    public CountAndSum deserialize(String topic, byte[] data) {
      return readValue(new String(data, UTF_8), CountAndSum.class);
    }
  }

  private static class ProfitPerTypeSerializer implements Serializer<ProfitPerType> {

    @Override
    public byte[] serialize(String topic, ProfitPerType data) {
      return writeValue(data).getBytes(UTF_8);
    }
  }

  private static class ProfitPerTypeDeserializer implements Deserializer<ProfitPerType> {

    @Override
    public ProfitPerType deserialize(String topic, byte[] data) {
      return readValue(new String(data, UTF_8), ProfitPerType.class);
    }
  }
}
