package info.sarihh.kafka.nine.utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import info.batey.kafka.unit.KafkaUnit;

/**
 * A unit testing class for Kafka Nine Utils.
 * 
 * @author Sari Haj Hussein
 */
public class KafkaNineUtilsTest {
    private KafkaUnit kafkaUnitServer;
    private KafkaNineUtils<String, String> kafkaUtils;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws IOException {
        kafkaUtils = KafkaNineUtils.getInstance();
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
        KafkaProducer<String, String> producer = kafkaUtils.createProducer();
        kafkaUtils.sendMessages(producer, messageList);
        KafkaConsumer<String, String> consumer = kafkaUtils.createConsumer();
        kafkaUtils.readMessages(consumer);
        kafkaUtils.deleteTopic();
    }

    @Test
    public void successfullySendReadMessagesNonDefaultTopic() throws TimeoutException {
        List<String> messageList = Arrays.asList("Message A", "Message B", "Message C");
        kafkaUtils.createTopic(kafkaUtils.getDefaultZookeeperServer(), "MyTopic");
        KafkaProducer<String, String> producer = kafkaUtils.createProducer();
        kafkaUtils.sendMessages(producer, "MyTopic", messageList);
        KafkaConsumer<String, String> consumer = kafkaUtils.createConsumer();
        kafkaUtils.readMessages(consumer, "MyTopic");
        kafkaUtils.deleteTopic(kafkaUtils.getDefaultZookeeperServer(), "MyTopic");
    }
}
