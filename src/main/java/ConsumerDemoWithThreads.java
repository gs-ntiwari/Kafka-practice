import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    public static void main(String[] args) {

        new ConsumerDemoWithThreads().run();
    }

    public void run()
    {
        BasicConfigurator.configure();

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        //create consumer properties
        String server="127.0.0.1:9092";
        String groupId="my_first_application";
        String offset="earliest";
        String topic="first_topic";

        CountDownLatch latch = new CountDownLatch(1);

        Runnable myConsumerThread = new ConsumerThread(server, groupId, offset, topic, latch);

        Thread thread = new Thread(myConsumerThread);

        //start the thread
        thread.start();

        //add a shutdown hook to shutdown the consumer properly

        Runtime.getRuntime().addShutdownHook(new Thread(()->
        {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) myConsumerThread).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application is closed");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        }
        finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerThread implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        private Logger logger= LoggerFactory.getLogger(ConsumerThread.class.getName());

        public ConsumerThread(String server, String groupId, String offset, String topic, CountDownLatch latch)
        {
            Properties properties=new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset);
            this.latch=latch;
            this.consumer=new KafkaConsumer<String, String>(properties);
            this.consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            //poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100l));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("key: " + record.key() + " value: " + record.value() + " partition: " + record.partition() + " offset: " + record.offset() + "\n");
                    }

                }
            }
            catch (WakeupException e)
            {
                logger.error("Received shutdown signal",e);
            }
            finally {
                //super important to close the consumer once we are done
                consumer.close();
                //tell the main method that we are done with the consumer
                latch.countDown();
            }

        }

        public void shutdown()
        {
            //the wakeup is a special method to interrupt the poll
            //it will throw wakeupException
            consumer.wakeup();

        }
    }
}
