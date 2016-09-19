package info.sarihh.kafka.eight.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import kafka.admin.TopicCommand;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringDecoder;
import kafka.serializer.StringEncoder;
import kafka.utils.VerifiableProperties;

/**
 * A Kafka utility class that uses the Kafka 0.8.2.X API (http://kafka.apache.org/082/documentation.html).
 * 
 * @author Sari Haj Hussein
 */
public class KafkaEightUtils<K, V> {
    private static final Logger logger = Logger.getLogger(KafkaEightUtils.class.getName());

    private KafkaEightUtils() {
        // defeat external instantiation
    }

    /** Returns an instance of the KafkaEightUtils class. */
    @SuppressWarnings("rawtypes")
    public static KafkaEightUtils getInstance() {
        return new KafkaEightUtils();
    }

    /** Gets the default Zookeeper server. */
    public String getDefaultZookeeperServer() {
        return "localhost:2181";
    }

    /** Gets the default Kafka server. */
    public String getDefaultKafkaServer() {
        return "localhost:9092";
    }

    /** Gets the default topic. */
    public String getDefaultTopic() {
        return "TestTopic";
    }

    /** Gets the default serializer.class. */
    public String getDefaultSerializerClass() {
        return StringEncoder.class.getName();
    }

    /** Gets the default request.required.acks. */
    public String getDefaultRequestRequiredAcks() {
        return "1";
    }

    /** Gets the default request.timeout.ms. */
    public String getDefaultRequestTimeoutMs() {
        return "50000";
    }

    /** Gets the default group.id. */
    public String getDefaultGroupId() {
        return "KafkaUtilsGroup";
    }

    /** Gets the default socket.timeout.ms. */
    public String getDefaultSocketTimeoutMs() {
        return "30000";
    }

    /** Gets the default consumer.id. */
    public String getDefaultConsumerId() {
        return "KafkaUtilsConsumer";
    }

    /** Gets the default auto.offset.reset. */
    public String getDefaultAutoOffsetReset() {
        return "smallest";
    }

    /** List the topics given the default Zookeeper server. */
    public void listTopics() {
        String[] arguments = new String[3];
        arguments[0] = "--list";
        arguments[1] = "--zookeeper";
        arguments[2] = getDefaultZookeeperServer();
        logger.info("Executing: ListTopics " + Arrays.toString(arguments));
        TopicCommand.main(arguments);
    }

    /** List the topics given the supplied zookeeperServer. */
    public void listTopics(String zookeeperServer) {
        String[] arguments = new String[3];
        arguments[0] = "--list";
        arguments[1] = "--zookeeper";
        arguments[2] = zookeeperServer;
        logger.info("Executing: ListTopics " + Arrays.toString(arguments));
        TopicCommand.main(arguments);
    }

    /** Creates the default topic given the deafault Zookeeper server. */
    public void createTopic() {
        String[] arguments = new String[9];
        arguments[0] = "--create";
        arguments[1] = "--zookeeper";
        arguments[2] = getDefaultZookeeperServer();
        arguments[3] = "--replication-factor";
        arguments[4] = "1";
        arguments[5] = "--partitions";
        arguments[6] = "1";
        arguments[7] = "--topic";
        arguments[8] = getDefaultTopic();
        logger.info("Executing: CreateTopic " + Arrays.toString(arguments));
        TopicCommand.main(arguments);
    }

    /** Creates the topic topicName given the supplied zookeeperServer. */
    public void createTopic(String zookeeperServer, String topicName) {
        String[] arguments = new String[9];
        arguments[0] = "--create";
        arguments[1] = "--zookeeper";
        arguments[2] = zookeeperServer;
        arguments[3] = "--replication-factor";
        arguments[4] = "1";
        arguments[5] = "--partitions";
        arguments[6] = "1";
        arguments[7] = "--topic";
        arguments[8] = topicName;
        logger.info("Executing: CreateTopic " + Arrays.toString(arguments));
        TopicCommand.main(arguments);
    }

    /** Deletes the default topic given the deafault Zookeeper server. */
    public void deleteTopic() {
        String[] arguments = new String[5];
        arguments[0] = "--delete";
        arguments[1] = "--zookeeper";
        arguments[2] = getDefaultZookeeperServer();
        arguments[3] = "--topic";
        arguments[4] = getDefaultTopic();
        logger.info("Executing: DeleteTopic " + Arrays.toString(arguments));
        TopicCommand.main(arguments);
    }

    /** Deletes the topic topicName given the supplied zookeeperServer. */
    public void deleteTopic(String zookeeperServer, String topicName) {
        String[] arguments = new String[5];
        arguments[0] = "--delete";
        arguments[1] = "--zookeeper";
        arguments[2] = zookeeperServer;
        arguments[3] = "--topic";
        arguments[4] = topicName;
        logger.info("Executing: DeleteTopic " + Arrays.toString(arguments));
        TopicCommand.main(arguments);
    }

    /** Creates a producer from the default properties. */
    public Producer<K, V> createProducer() {
        Properties props = new Properties();
        props.put("metadata.broker.list", getDefaultKafkaServer());
        props.put("serializer.class", getDefaultSerializerClass());
        props.put("request.required.acks", getDefaultRequestRequiredAcks());
        props.put("request.timeout.ms", getDefaultRequestTimeoutMs());
        ProducerConfig config = new ProducerConfig(props);
        return new Producer<K, V>(config);
    }

    /** Creates a producer from the supplied properties. */
    public Producer<K, V> createProducer(String kafkaServer, String serializerClass, String requestRequiredAcks, String requestTimeoutMs) {
        Properties props = new Properties();
        props.put("metadata.broker.list", kafkaServer);
        props.put("serializer.class", serializerClass);
        props.put("request.required.acks", requestRequiredAcks);
        props.put("request.timeout.ms", requestTimeoutMs);
        ProducerConfig config = new ProducerConfig(props);
        return new Producer<K, V>(config);
    }

    /** Creates a producer from the supplied properties. */
    public Producer<K, V> createProducer(Properties props) {
        return new Producer<K, V>(new ProducerConfig(props));
    }

    /** Creates a consumer from the default properties. */
    public ConsumerConnector createConsumer() {
        Properties props = new Properties();
        props.put("zookeeper.connect", getDefaultZookeeperServer());
        props.put("group.id", getDefaultGroupId());
        props.put("socket.timeout.ms", getDefaultSocketTimeoutMs());
        props.put("consumer.id", getDefaultConsumerId());
        props.put("auto.offset.reset", getDefaultAutoOffsetReset());
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
    }

    /** Creates a consumer from the supplied properties. */
    public ConsumerConnector createConsumer(String zookeeperServer, String groupId, String socketTimeoutMs, String consumerId, String autoOffsetReset) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeperServer);
        props.put("group.id", groupId);
        props.put("socket.timeout.ms", socketTimeoutMs);
        props.put("consumer.id", consumerId);
        props.put("auto.offset.reset", autoOffsetReset);
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
    }

    /** Creates a consumer from the supplied Properties object. */
    public ConsumerConnector createConsumer(Properties props) {
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
    }

    /** Sends messages to the default topic using producer. */
    @SuppressWarnings("unchecked")
    public void sendMessages(Producer<K, V> producer, V... messages) {
        for (V message : messages) {
            KeyedMessage<K, V> keyedMessage = new KeyedMessage<>(getDefaultTopic(), message);
            producer.send(keyedMessage);
            logger.info("Published message " + message + " to topic " + getDefaultTopic() + ".");
        }
        producer.close();
    }

    /** Sends messages to the topic topicName using producer. */
    @SuppressWarnings("unchecked")
    public void sendMessages(Producer<K, V> producer, String topicName, V... messages) {
        for (V message : messages) {
            KeyedMessage<K, V> keyedMessage = new KeyedMessage<>(topicName, message);
            producer.send(keyedMessage);
            logger.info("Published message " + message + " to topic " + topicName + ".");
        }
        producer.close();
    }

    /** Sends messageList to the default topic using producer. */
    public void sendMessages(Producer<K, V> producer, List<V> messageList) {
        for (V message : messageList) {
            KeyedMessage<K, V> keyedMessage = new KeyedMessage<>(getDefaultTopic(), message);
            producer.send(keyedMessage);
            logger.info("Published message " + message + " to topic " + getDefaultTopic() + ".");
        }
        producer.close();
    }

    /** Sends messageList to the topic topicName using producer. */
    public void sendMessages(Producer<K, V> producer, String topicName, List<V> messageList) {
        for (V message : messageList) {
            KeyedMessage<K, V> keyedMessage = new KeyedMessage<>(topicName, message);
            producer.send(keyedMessage);
            logger.info("Published message " + message + " to topic " + topicName + ".");
        }
        producer.close();
    }

    /** Reads expectedMessages number of messages from the default topic. This consumer is threaded. */
    public List<String> readMessages(ConsumerConnector consumer, final int expectedMessages) throws TimeoutException {
        ExecutorService singleThread = Executors.newSingleThreadExecutor();
        StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties(new Properties()));
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(getDefaultTopic(), 1);
        Map<String, List<KafkaStream<String, String>>> events = consumer.createMessageStreams(topicMap, stringDecoder, stringDecoder);
        List<KafkaStream<String, String>> events1 = events.get(getDefaultTopic());
        final KafkaStream<String, String> kafkaStreams = events1.get(0);

        Future<List<String>> submit = singleThread.submit(new Callable<List<String>>() {
            public List<String> call() throws Exception {
                List<String> messages = new ArrayList<>();
                ConsumerIterator<String, String> iterator = kafkaStreams.iterator();
                while (messages.size() != expectedMessages && iterator.hasNext()) {
                    String message = iterator.next().message();
                    logger.info("Received message: " + message + ".");
                    messages.add(message);
                }
                return messages;
            }
        });

        try {
            return submit.get(30, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new TimeoutException("Timed out waiting for messages");
        } finally {
            singleThread.shutdown();
            consumer.shutdown();
        }
    }

    /** Reads expectedMessages number of messages from the topic topicName. This consumer is threaded. */
    public List<String> readMessages(ConsumerConnector consumer, String topicName, final int expectedMessages) throws TimeoutException {
        ExecutorService singleThread = Executors.newSingleThreadExecutor();
        StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties(new Properties()));
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(topicName, 1);
        Map<String, List<KafkaStream<String, String>>> events = consumer.createMessageStreams(topicMap, stringDecoder, stringDecoder);
        List<KafkaStream<String, String>> events1 = events.get(topicName);
        final KafkaStream<String, String> kafkaStreams = events1.get(0);

        Future<List<String>> submit = singleThread.submit(new Callable<List<String>>() {
            public List<String> call() throws Exception {
                List<String> messages = new ArrayList<>();
                ConsumerIterator<String, String> iterator = kafkaStreams.iterator();
                while (messages.size() != expectedMessages && iterator.hasNext()) {
                    String message = iterator.next().message();
                    logger.info("Received message: " + message + ".");
                    messages.add(message);
                }
                return messages;
            }
        });

        try {
            return submit.get(30, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new TimeoutException("Timed out waiting for messages");
        } finally {
            singleThread.shutdown();
            consumer.shutdown();
        }
    }
}