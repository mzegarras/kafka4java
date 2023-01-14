package csonic.kafka01;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        var servers = "localhost:29092";
        var appId = "galaxy-application-01";
        var topic = "weblog";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,servers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,appId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);


        //subscriber consumer

        consumer.subscribe(Collections.singleton(topic));

        //poll for new data
        while (true){
            var records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String,String> record: records){
                logger.info("Key" + record.key() + " - Value:" + record.value());
                logger.info("Partition" + record.partition() + " - Offset:" + record.offset());
            }
        }
    }
}
