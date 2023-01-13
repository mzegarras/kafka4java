package csonic.kafka01;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        Properties properties = new Properties();
        var bootstrapServers = "localhost:29092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);


        var topic = "weblog";

        for (int i = 0; i <= 10; i++) {

            var value = "Lab Galaxt-" + i;
            var key =  "id_" + i;

            ProducerRecord<String,String> record = new ProducerRecord<String,String>(topic,key,value);


            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is successfully sent or an exception is thrown
                    if(e==null){
                        //the record was successfully sent
                        logger.info("Receive new metadata: ================="  );
                        logger.info("Key:" + key );
                        logger.info("Topic:" + recordMetadata.topic()  );
                        logger.info("Partition:" + recordMetadata.partition()  );
                        logger.info("Offset:" + recordMetadata.offset()  );
                        logger.info("Timestamp:" + recordMetadata.timestamp() );
                        logger.info(" ================="  );
                    }else{

                    }
                }
            }).get();
        }


        producer.flush();

        producer.close();
    }
}
