package info.sarihh.kafka.nine.utils;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;

/**
 * A thread-safe Kafka consumer.
 * 
 * @author Sari Haj Hussein
 */
public class KafkaConsumerThread<K, V> extends Thread {
    private static final Logger logger = Logger.getLogger(KafkaConsumerThread.class.getName());

    private AtomicBoolean closed = new AtomicBoolean(false);
    private String topicName;
    private KafkaConsumer<K, V> consumer;

    public KafkaConsumerThread(KafkaConsumer<K, V> consumer, String topicName) {
        this.topicName = topicName;
        this.consumer = consumer;
    }

    public void run() {
        try {
            consumer.subscribe(Arrays.asList(topicName));
            int timeouts = 0;
            while (!closed.get()) {
                ConsumerRecords<K, V> records = consumer.poll(100);
                if (records.count() == 0) {
                    timeouts++;
                } else {
                    logger.info(String.format("Got %d records after %d timeouts.\n", records.count(), timeouts));
                    timeouts = 0;
                }
                for (ConsumerRecord<K, V> record : records) {
                    logger.info(String.format("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value()));
                }
            }
        } catch (WakeupException e) {
            if (!closed.get()) {
                throw e;
            }
        } finally {
            consumer.close();
        }
    }

    /** Shutdown hook which can be called from a separate thread. */
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}