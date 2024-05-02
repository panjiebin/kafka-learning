package com.pan.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Kafka事务实现端到端写的一次性（该案例还有点瑕疵）
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
        // 开启幂等性需要满足一下配置:
        // retries > 0
        // acks = -1或all
        // max.in.flight.requests.per.connection <= 5
        props2.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        props2.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props2.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "4");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props2);

        // 1. 初始化事务
        producer.initTransactions();

        consumer.subscribe(Collections.singletonList("topic-a"));
        boolean flag = true;
        while (flag) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            producer.beginTransaction();
            try {
                // 从拉取到的数据中，获得本批数据包含哪些分区
                Set<TopicPartition> topicPartitions = records.partitions();
                for (TopicPartition topicPartition : topicPartitions) {
                    // 从拉取到的数据中取到本分区的所有数据
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        String result = record.value().toUpperCase();
                        ProducerRecord<String, String> resultRecord = new ProducerRecord<>("topic-b", result);
                        producer.send(resultRecord);
                    }
                }
                // 提交偏移量，这边还不能完全做到EOS语义
                // 比如当偏移量提交成功后，程序崩溃，事务没提交成功，就会丢数据了
                consumer.commitSync();

                // 提交事务
                producer.commitTransaction();
            } catch (Exception e) {
                // 事务回滚
                // 下游消费者可以通过设置isolation.level=read_committed来避开本地产生的“脏数据”
                producer.abortTransaction();
            }
        }

        consumer.close();
        producer.close();
    }
}
