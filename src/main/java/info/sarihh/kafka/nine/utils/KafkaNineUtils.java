package info.sarihh.kafka.nine.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import kafka.admin.TopicCommand;

/**
 * A Kafka utility class that uses the Kafka 0.9.0.X API (http://kafka.apache.org/090/documentation.html).
 * 
 * @author Sari Haj Hussein
 */
public class KafkaNineUtils<K, V> {
    private static final Logger logger = Logger.getLogger(KafkaNineUtils.class.getName());

    private KafkaNineUtils() {
        // defeat external instantiation
    }

    /** Returns an instance of the KafkaNineUtils class. */
    @SuppressWarnings("rawtypes")
    public static KafkaNineUtils getInstance() {
        return new KafkaNineUtils();
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

    /** Gets the default acks value. */
    public String getDefaultAcks() {
        return "all";
    }

    /** Gets the default buffer.memory value. */
    public String getDefaultBufferMemory() {
        return "33554432";
    }

    /** Gets the default compression.type value. */
    public String getDefaultCompressionType() {
        return "gzip";
    }

    /** Gets the default retries value. */
    public String getDefaultRetries() {
        return "0";
    }

    /** Gets the default batch.size value. */
    public String getDefaultBatchSize() {
        return "16384";
    }

    /** Gets the default client.id value. */
    public String getDefaultClientId() {
        return "KafkUtilsClient";
    }

    /** Gets the default connections.max.idle.ms value. */
    public String getDefaultConnectionsMaxIdleMs() {
        return "540000";
    }

    /** Gets the default linger.ms value. */
    public String getDefaultLingerMs() {
        return "0";
    }

    /** Gets the default request.timeout.ms value. */
    public String getDefaultRequestTimeoutMs() {
        return "30000";
    }

    /** Gets the default key.serializer value. */
    public String getDefaultKeySerializer() {
        return StringSerializer.class.getName();
    }

    /** Gets the default value.serializer value. */
    public String getDefaultValueSerializer() {
        return StringSerializer.class.getName();
    }

    /** Gets the default group.id value. */
    public String getDefaultGroupId() {
        return "KafkUtilsGroup";
    }

    /** Gets the default enable.auto.commit value. */
    public String getDefaultEnableAutoCommit() {
        return "true";
    }

    /** Gets the default auto.commit.interval.ms value. */
    public String getDefaultAutoCommitIntervalMs() {
        return "1000";
    }

    /** Gets the default session.timeout.ms value. */
    public String getDefaultSessionTimeoutMs() {
        return "30000";
    }

    /** Gets the default key.deserializer value. */
    public String getDefaultKeyDeserializer() {
        return StringDeserializer.class.getName();
    }

    /** Gets the default value.deserializer value. */
    public String getDefaultValueDeserializer() {
        return StringDeserializer.class.getName();
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
    public KafkaProducer<K, V> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", getDefaultKafkaServer());
        props.put("acks", getDefaultAcks());
        props.put("buffer.memory", getDefaultBufferMemory());
        props.put("compression.type", getDefaultCompressionType());
        props.put("retries", getDefaultRetries());
        props.put("batch.size", getDefaultBatchSize());
        props.put("client.id", getDefaultClientId());
        props.put("connections.max.idle.ms", getDefaultConnectionsMaxIdleMs());
        props.put("linger.ms", getDefaultLingerMs());
        props.put("request.timeout.ms", getDefaultRequestTimeoutMs());
        props.put("key.serializer", getDefaultKeySerializer());
        props.put("value.serializer", getDefaultValueSerializer());
        return new KafkaProducer<K, V>(props);
    }

    /** Creates a producer from the supplied properties. */
    public KafkaProducer<K, V> createProducer(String kafkaServer, String acks, String bufferMemory, String compressionType, String retries, String batchSize,
        String clientId, String connectionsMaxIdleMs, String lingerMs, String requestTimeoutMs, String keySerializer, String valueSerializer) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaServer);
        props.put("acks", acks);
        props.put("buffer.memory", bufferMemory);
        props.put("compression.type", compressionType);
        props.put("retries", retries);
        props.put("batch.size", batchSize);
        props.put("client.id", clientId);
        props.put("connections.max.idle.ms", connectionsMaxIdleMs);
        props.put("linger.ms", lingerMs);
        props.put("request.timeout.ms", requestTimeoutMs);
        props.put("key.serializer", keySerializer);
        props.put("value.serializer", valueSerializer);
        return new KafkaProducer<K, V>(props);
    }

    /** Creates a producer from the supplied Properties object. */
    public KafkaProducer<K, V> createProducer(Properties props) {
        return new KafkaProducer<K, V>(props);
    }

    /** Creates a consumer from the default properties. */
    public KafkaConsumer<K, V> createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", getDefaultKafkaServer());
        props.put("group.id", getDefaultGroupId());
        props.put("enable.auto.commit", getDefaultEnableAutoCommit());
        props.put("auto.commit.interval.ms", getDefaultAutoCommitIntervalMs());
        props.put("session.timeout.ms", getDefaultSessionTimeoutMs());
        props.put("key.deserializer", getDefaultKeyDeserializer());
        props.put("value.deserializer", getDefaultValueDeserializer());
        return new KafkaConsumer<K, V>(props);
    }

    /** Creates a consumer from the supplied properties. */
    public KafkaConsumer<K, V> createConsumer(String kafkaServer, String groupId, String enableAutoCommit, String autoCommitIntervalMs, String sessionTimeoutMs,
        String keyDeserializer, String valueDeserializer) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaServer);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", enableAutoCommit);
        props.put("auto.commit.interval.ms", autoCommitIntervalMs);
        props.put("session.timeout.ms", sessionTimeoutMs);
        props.put("key.deserializer", keyDeserializer);
        props.put("value.deserializer", valueDeserializer);
        return new KafkaConsumer<K, V>(props);
    }

    /** Creates a consumer from the supplied Properties object. */
    public KafkaConsumer<K, V> createConsumer(Properties props) {
        return new KafkaConsumer<K, V>(props);
    }

    /** Sends messages to the default topic using producer. */
    @SuppressWarnings("unchecked")
    public void sendMessages(KafkaProducer<K, V> producer, V... messages) {
        for (V message : messages) {
            ProducerRecord<K, V> producerRecord = new ProducerRecord<>(getDefaultTopic(), message);
            producer.send(producerRecord);
            logger.info("Published message " + message + " to topic " + getDefaultTopic() + ".");
        }
        producer.close();
    }

    /** Sends messages to the topic topicName using producer. */
    @SuppressWarnings("unchecked")
    public void sendMessage(KafkaProducer<K, V> producer, String topicName, V... messages) {
        for (V message : messages) {
            ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topicName, message);
            producer.send(producerRecord);
            logger.info("Published message " + message + " to topic " + topicName + ".");
        }
        producer.close();
    }

    /** Sends messageList to the default topic using producer. */
    public void sendMessages(KafkaProducer<K, V> producer, List<V> messageList) {
        for (V message : messageList) {
            ProducerRecord<K, V> producerRecord = new ProducerRecord<>(getDefaultTopic(), message);
            producer.send(producerRecord);
            logger.info("Published message " + message + " to topic " + getDefaultTopic() + ".");
        }
        producer.close();
    }

    /** Sends messageList to the topic topicName using producer. */
    public void sendMessages(KafkaProducer<K, V> producer, String topicName, List<V> messageList) {
        for (V message : messageList) {
            ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topicName, message);
            producer.send(producerRecord);
            logger.info("Published message " + message + " to topic " + topicName + ".");
        }
        producer.close();
    }

    /** Reads messages from the default topic. This consumer is threaded. */
    public void readMessages(KafkaConsumer<K, V> consumer) {
        KafkaConsumerThread<K, V> kct = new KafkaConsumerThread<K, V>(consumer, getDefaultTopic());
        kct.start();
    }

    /** Reads messages from the topic topicName. This consumer is threaded. */
    public void readMessages(KafkaConsumer<K, V> consumer, String topicName) {
        KafkaConsumerThread<K, V> kct = new KafkaConsumerThread<K, V>(consumer, topicName);
        kct.start();
    }
}