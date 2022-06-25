package myapps;

import java.time.Instant;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.json.JSONObject;

public class BankCount {
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-bankcount");
      props.put(StreamsConfig.EXACTLY_ONCE, "true");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    final StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> source = builder.stream("streams-bank-input");
//    KStream<String, String> bankTrans = builder.stream("streams-bank-input");

    // initial
    JSONObject initAgg = new JSONObject();
    initAgg.put("count", 0);
    initAgg.put("balance", 0);
    initAgg.put("time", Instant.ofEpochMilli(0L).toString());

    KTable<String, String> bankCount = source.groupByKey()//Serialized.with(Serdes.String(), Serdes.String());
        .aggregate(
            () -> initAgg.toString(),
            (String key, String trans, String balance) -> newBalance(trans, balance)
        );
    bankCount.toStream().to("streams-bank-output");

    final Topology topology = builder.build();
    System.out.println(topology.describe());

    KafkaStreams streams = new KafkaStreams(topology, props);
      final CountDownLatch latch = new CountDownLatch(1);

      Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown") {
          @Override
          public void run() {
              streams.close();
              latch.countDown();
          }
      });

      try {
          streams.start();
          latch.await();
      } catch (Throwable e) {
          System.exit(1);
      }
      System.exit(0);
  }

  static private String newBalance (String trans, String balance) {
      JSONObject oldBalance = new JSONObject(balance);
      Integer oldCount = oldBalance.getInt("count");
      Integer oldBalanceValue = oldBalance.getInt("balance");

      JSONObject transObj = new JSONObject(trans);
      Long transTime = Instant.parse(transObj.getString("time")).toEpochMilli();
      Long balanceTime = Instant.parse(oldBalance.getString("time")).toEpochMilli();
      System.out.println(balanceTime);

      JSONObject newBalance = new JSONObject();
      newBalance.put("count", oldCount + 1);
      newBalance.put("balance", transObj.getInt("amount") + oldBalanceValue);
      newBalance.put("time", Instant.ofEpochMilli(Math.max(transTime, balanceTime)).toString());

      return newBalance.toString();
  }
}
