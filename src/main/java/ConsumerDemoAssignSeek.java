import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {
        BasicConfigurator.configure();

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

        //create consumer properties
        String server="127.0.0.1:9092";
        String offset="earliest";
        String topic="first_topic";

        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset);

        //create a consumer
        KafkaConsumer<String, String> consumer=new KafkaConsumer<String, String>(properties);

        //assign and seek are mostly use for replay data or fetch a specific data

        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        long offsetToReadFrom=15L;
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int noOfMessagesToRead=5;
        boolean keepOnReading=true;
        int noOfMessagesReadSoFar=0;

        //poll for new data
        while(keepOnReading)
        {
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100l));
            for(ConsumerRecord<String, String> record:records)
            {
                noOfMessagesReadSoFar+=1;
                logger.info("key: "+record.key()+" value: "+record.value()+" partition: "+record.partition()+" offset: "+record.offset()+"\n");
                if(noOfMessagesReadSoFar>noOfMessagesToRead)
                {
                    keepOnReading=false;
                    break;
                }
            }

        }


    }


}
