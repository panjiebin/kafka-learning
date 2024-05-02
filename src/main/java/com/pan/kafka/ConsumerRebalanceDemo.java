package com.pan.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

/**
 * 消费者组再均衡
 * @author panjb
 */
public class ConsumerRebalanceDemo {

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.load(ConsumerRebalanceDemo.class.getClassLoader().getResourceAsStream("kafka-consumer.properties"));
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "g2");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList("test2"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("我被取消了如下主题分区：" + partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("我被重新分配了如下主题分区：" + partitions);
            }
        });

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
