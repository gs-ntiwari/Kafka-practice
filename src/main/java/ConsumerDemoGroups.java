import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups {
    public static void main(String[] args) {
        BasicConfigurator.configure();

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        //create consumer properties
        String server="127.0.0.1:9092";
        String groupId="my_first_application";
        String offset="earliest";
        String topic="first_topic";


        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset);

        //create a consumer
        KafkaConsumer<String, String> consumer=new KafkaConsumer<String, String>(properties);

        //subscribe consumer to topics
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data
        while(true)
        {
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(5000l));
            for(ConsumerRecord<String, String> record:records)
            {
                logger.debug("key: "+record.key()+" value: "+record.value()+" partition: "+record.partition()+" offset: "+record.offset()+"\n");
            }

        }


    }
}
