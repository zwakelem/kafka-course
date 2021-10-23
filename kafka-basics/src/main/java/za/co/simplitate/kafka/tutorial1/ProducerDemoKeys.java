package za.co.simplitate.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);



        for(int i=0; i<10; i++) {
            // create a producer record
            String topic = "first_topic";
            String value = "first_topic " + i;
            String key = "id_" + i;

            ProducerRecord<String, String> record = new ProducerRecord<>(topic,key,value);
            LOG.info("key: {}",key);
            /*
                id_0 p1
                id_1 p0
                id_2 p2
                id_3 p0
                id_4 p2
                id_5 p2
                id_6 p0
                id_7 p2
                id_8 p1
                id_9 p2
             */

            // send data - asynchronous
            producer.send(record, (recordMetadata, e) -> {
                if(e == null) {
                    LOG.info("Received new metadata. \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    LOG.error("Error sending message: {}", e.toString());
                }
            }).get(); // make send synchronous, degrades performance
        }


        // flush data
        producer.flush();

        // flush and close
        producer.close();
    }
}
