package tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);



        for(int i=0; i<10; i++) {

            // create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic",
                    "hello world- " + i);

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
            });
        }


        // flush data
        producer.flush();

        // flush and close
        producer.close();
    }
}
