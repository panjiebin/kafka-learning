package com.pan.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

/**
 * @author panjb
 */
public class ConsumerDemo {

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.load(ConsumerDemo.class.getClassLoader().getResourceAsStream("kafka-consumer.properties"));
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "gg2");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("test"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();
                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();

                // 在kafka的数据底层存储中，不光有用户的业务数据，还有大量元数据
                // timestamp是记录本条数据的时间戳
                // 时间戳有2中类型：本条数据的创建时间（生产者），本条数据的追加时间（broker写入log文件的时间）
                Optional<Integer> leaderEpoch = record.leaderEpoch();
                TimestampType timestampType = record.timestampType();
                long timestamp = record.timestamp();

                // 数据头
                // 数据头是生产者在写入数据时附加的（相当于自己自定义元数据）
                Headers headers = record.headers();

                System.out.printf("key: %s, value: %s, topic: %s, partition: %d, offset: %d, " +
                        "leaderEpoch: %s, timestamp type: %s, timestamp: %d%n", key, value, topic, partition, offset,
                        leaderEpoch.toString(), timestampType.name, timestamp);
            }
        }
    }
}
