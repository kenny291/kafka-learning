package myapps;

//import util.properties packages
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.json.JSONObject;
//import simple producer packages
import org.apache.kafka.clients.producer.Producer;
//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;
//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
//Create java class named “SimpleProducer”
public class BankProducer {

    public static void main(String[] args) throws Exception{

        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 3);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer(props);

        for(int i = 0; i < 100000; i++)
            producer.send(sqsMsg());
//            Thread.sleep(100);

        System.out.println("Message sent successfully");
        producer.close();
    }
    static private ProducerRecord createMsg() {
        ArrayList names = new ArrayList();
        names.add("John");
        names.add("Tom");
        names.add("Kenny");
        names.add("Lee");
        names.add("Andrew");
        names.add("Yanjun");

        JSONObject msgJson = new JSONObject();
        String name = (String) names.get(new Random().nextInt(names.size()));
        msgJson.put("Name", name);
        msgJson.put("amount", new Random().nextInt(9000));
        String currentTime = Instant.now().toString();
        msgJson.put("time", currentTime);
        System.out.println(msgJson);

        return new ProducerRecord("streams-bank-input",name, msgJson.toString());
    }

    static private ProducerRecord sqsMsg() {
        String sqs = "{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"ap-southeast-1\",\"eventTime\":\"2022-01-19T10:02:44.116Z\",\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:AROASPFTUGX5C3PTDKFSO:aws-sdk-php-1642584902001\"},\"requestParameters\":{\"sourceIPAddress\":\"10.103.209.171\"},\"responseElements\":{\"x-amz-request-id\":\"YJ3DQB3HKW84WMEM\",\"x-amz-id-2\":\"arnVhgNW3AcJf9kDKW8/w6Zv0urNvw+PKWxKyuGV+4Ae62an5fCQfkE2y5cIMdlNlPJhYnL/cBocuv952kreXa2HBQDmKB/I\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"commission-upload-event\",\"bucket\":{\"name\":\"shopback-commissions-prod-id\",\"ownerIdentity\":{\"principalId\":\"A2VBHXFAQP3NOR\"},\"arn\":\"arn:aws:s3:::shopback-commissions-prod-id\"},\"object\":{\"key\":\"commissionsdb/ShopeeID/45182117-lastrawdata.json\",\"size\":1330,\"eTag\":\"a731b6dc26cb57ef31430cf44afbd707\",\"versionId\":\"j755BV_iCkrzgWt9TNUX.q9aVNVaclVV\",\"sequencer\":\"0061E7E1C4189118F5\"}}}]}";

        return new ProducerRecord("sqs.prod-id-shopback-commissions-last-raw-data-s3-upload-events","name", sqs);
    }
}