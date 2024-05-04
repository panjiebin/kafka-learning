package com.pan.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

/**
 * Kafka事务实现端到端写的一次性
 * <p>consume-transform-produce
 * @author panjb
 */
public class KafkaTransactionDemo {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092,node3:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "trans-test");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        Properties props2 = new Properties();
        props2.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092,node3:9092");
        props2.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props2.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 配置事务id
        props2.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "trans-001");
        // 使用事务必须开启幂等性
        props2.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props2);

        // 1. 初始化事务
        producer.initTransactions();

        consumer.subscribe(Collections.singletonList("topic-source"));
        boolean flag = true;
        while (flag) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            producer.beginTransaction();
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            if (!records.isEmpty()) {
                try {
                    // 从拉取到的数据中，获得本批数据包含哪些分区
                    Set<TopicPartition> topicPartitions = records.partitions();
                    for (TopicPartition topicPartition : topicPartitions) {
                        // 从拉取到的数据中取到本分区的所有数据
                        List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
                        for (ConsumerRecord<String, String> record : partitionRecords) {
                            String result = record.value().toUpperCase();
                            ProducerRecord<String, String> resultRecord = new ProducerRecord<>("topic-sink", result);
                            producer.send(resultRecord);
                        }
                        long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                        offsets.put(topicPartition, new OffsetAndMetadata(lastConsumedOffset + 1));
                    }

                    // 提交消费位移
                    producer.sendOffsetsToTransaction(offsets, "trans-test");

                    // 提交事务
                    producer.commitTransaction();
                } catch (Exception e) {
                    // 终止事务
                    // 下游消费者可以通过设置isolation.level=read_committed来避开本地产生的“脏数据”
                    producer.abortTransaction();
                }
            }
        }

        consumer.close();
        producer.close();
    }
}
