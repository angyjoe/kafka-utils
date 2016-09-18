package info.sarihh.kafka.eight.utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import info.batey.kafka.unit.KafkaUnit;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;

/**
 * A unit testing class for Kafka Eight Utils.
 * 
 * @author Sari Haj Hussein
 */
public class KafkaEightUtilsTest {
    private KafkaUnit kafkaUnitServer;
    private KafkaEightUtils<String, String> kafkaUtils;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws IOException {
        kafkaUtils = KafkaEightUtils.getInstance();
        kafkaUnitServer = new KafkaUnit(2181, 9092);
        kafkaUnitServer.startup();
    }

    @After
    public void cleanup() {
        kafkaUnitServer.shutdown();
    }

    @Test
    public void successfullyListTopics() {
        kafkaUtils.listTopics();
    }

    @Test
    public void successfullyCreateDeleteDefaultTopic() {
        kafkaUtils.createTopic();
        kafkaUtils.listTopics();
        kafkaUtils.deleteTopic();
        kafkaUtils.listTopics();
    }

    @Test
    public void successfullyCreateDeleteNonDefaultTopic() {
        kafkaUtils.createTopic(kafkaUtils.getDefaultZookeeperServer(), "MyTopic");
        kafkaUtils.listTopics();
        kafkaUtils.deleteTopic(kafkaUtils.getDefaultZookeeperServer(), "MyTopic");
        kafkaUtils.listTopics();
    }

    @Test
    public void successfullySendReadMessagesDefaultTopic() throws TimeoutException {
        List<String> messageList = Arrays.asList("Message A", "Message B", "Message C");
        kafkaUtils.createTopic();
        Producer<String, String> producer = kafkaUtils.createProducer();
        kafkaUtils.sendMessages(producer, messageList);
        ConsumerConnector consumer = kafkaUtils.createConsumer();
        kafkaUtils.readMessages(consumer, 3);
        kafkaUtils.deleteTopic();
    }

    @Test
    public void successfullySendReadMessagesNonDefaultTopic() throws TimeoutException {
        List<String> messageList = Arrays.asList("Message A", "Message B", "Message C");
        kafkaUtils.createTopic(kafkaUtils.getDefaultZookeeperServer(), "MyTopic");
        Producer<String, String> producer = kafkaUtils.createProducer();
        kafkaUtils.sendMessages(producer, "MyTopic", messageList);
        ConsumerConnector consumer = kafkaUtils.createConsumer();
        kafkaUtils.readMessages(consumer, "MyTopic", 3);
        kafkaUtils.deleteTopic(kafkaUtils.getDefaultZookeeperServer(), "MyTopic");
    }
}
