package myapps;

import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class WordCount {
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-wordcount1");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

//      ClassLoader classLoader = WordCount.class.getClassLoader();
//      URL file = classLoader.getResource("log4j.properties");
//      System.out.println(file + "sss9");

      final StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> source = builder.stream("streams-plaintext-input");
    source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault())
        .split("\\W+")))
        .groupBy((key, value) -> value)
        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>> as ("count-store"))
        .toStream()  // convert KTable to KStream
      .to("streams-wordcount-output1", Produced.with(Serdes.String(), Serdes.Long()));

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
}
