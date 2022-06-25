//package myapps;
//
//import io.confluent.kafka.streams.serdes.avro.PrimitiveAvroSerde;
//import java.util.Collections;
//import java.util.Map;
//import java.util.Properties;
//
//import org.apache.avro.generic.IndexedRecord;
//import org.apache.kafka.clients.consumer.Consumer;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.kafka.common.serialization.Serde;
//
//
//public class KafkaConsumer<G extends IndexedRecord, G1 extends IndexedRecord> {
//  public static void main(String[] args) {
//     runConsumer();
//  }
//
//  static void runConsumer() {
//    Consumer<GenericRecord, GenericRecord> consumer = ConsumerCreator.createConsumer();
//
//    int noMessageFound = 0;
//
//    while (true) {
//      ConsumerRecords<GenericRecord, GenericRecord> consumerRecords = consumer.poll(1000);
//      // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
//      if (consumerRecords.count() == 0) {
//        noMessageFound++;
//        if (noMessageFound > 100)
//          // If no message found count is reached to threshold exit loop.
//          break;
//        else continue;
//      }
//
//      // print each record.
//      consumerRecords.forEach(
//          record -> {
//            System.out.println("Record Key " + record.key());
//            System.out.println("Record value " + record.value());
//            System.out.println("Record partition " + record.partition());
//            System.out.println("Record offset " + record.offset());
//          });
//
//      // commits the offset of record to broker.
//      consumer.commitAsync();
//    }
//    consumer.close();
//  }
//
//    public static class ConsumerCreator {
//
//        public static Consumer<GenericRecord, GenericRecord> createConsumer() {
//            Properties props = new Properties();
//            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//            props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_consumer");
//            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, GenericAvroSerde.class);
//            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroSerde.class);
//            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
//            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//
//            final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
//                registry);
//            final Deser<GenericRecord> keyGenericAvroSerde = new GenericAvroSerde();
//            keyGenericAvroSerde.configure(serdeConfig, true);
//            final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
//            valueGenericAvroSerde.configure(serdeConfig, false);
//
////            Consumer<GenericRecord, GenericRecord> consumer = new KafkaConsumer<>(props);
////            consumer.subscribe(Collections.singletonList("__cdc_bad_records"), );
//            return consumer;
//        }
//
//    }
//}
