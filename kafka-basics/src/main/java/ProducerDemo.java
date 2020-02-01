import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        BasicConfigurator.configure();

        final Logger logger =LoggerFactory.getLogger(ProducerWithCallBackAndKeys.class);
        //create producer properties
        String server="127.0.0.1:9092";
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create a producer
        KafkaProducer<String, String> producer= new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++) {
            String value="hello world " + i;
            String topic ="first_topic";
            String key="key_"+i;
            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("key "+key);
            //send data -async, .get() makes it sync
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime a record is sent successfully or exception occurred
                    if (e == null) {
                        logger.info("Received a new metadata \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Timestamp:" + recordMetadata.timestamp());
                    } else {
                        logger.error("Error occurred while producing:", e);
                    }

                }
            }); //block the .send()to make it synchronous -- don't do this in production
        }

        producer.flush();

        producer.close();
}
}
