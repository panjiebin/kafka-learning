package com.pan.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

/**
 * 消费者指定offset进行读取
 * @author panjb
 */
public class ConsumerOffsetDemo {

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.load(ConsumerOffsetDemo.class.getClassLoader().getResourceAsStream("kafka-consumer.properties"));
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "gg2");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        TopicPartition topicPartition = new TopicPartition("test", 0);

        // 指定一个确定的起始消费位置，不要使用 subscribe 来订阅主题
        consumer.assign(Collections.singletonList(topicPartition));
        consumer.seek(topicPartition, 5);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();
                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();

                Optional<Integer> leaderEpoch = record.leaderEpoch();
                TimestampType timestampType = record.timestampType();
                long timestamp = record.timestamp();

                System.out.printf("key: %s, value: %s, topic: %s, partition: %d, offset: %d, " +
                        "leaderEpoch: %s, timestamp type: %s, timestamp: %d%n", key, value, topic, partition, offset,
                        leaderEpoch.toString(), timestampType.name, timestamp);
            }
        }
    }
}
