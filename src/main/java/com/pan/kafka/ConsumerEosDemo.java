package com.pan.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.*;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * 端到端精确一次示例：消费者读取kafka数据，写入mysql，并将offset写入mysql
 * <p>
 * 利用mysql的事务机制，保证数据写入和offset写入的原子性
 * @author panjb
 */
public class ConsumerEosDemo {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092,node3:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "eoc-g");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:mysql://node1:3306/test", "root", "123456");

        PreparedStatement stuPstat = conn.prepareStatement("INSERT INTO t_student VALUES(?,?,?,?)");

        PreparedStatement offsetPstat = conn.prepareStatement("INSERT INTO t_offsets VALUES(?,?) ON DUPLICATE KEY UPDATE offset=?");

        PreparedStatement offsetQueryPstat = conn.prepareStatement("SELECT offset FROM t_offsets WHERE topic_partition=?");

        conn.setAutoCommit(false);

        consumer.subscribe(Collections.singletonList("stu-info"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("触发分区再平衡");
                try {
                    for (TopicPartition partition : partitions) {
                        offsetQueryPstat.setString(1, String.format("%s:%d", partition.topic(), partition.partition()));
                        ResultSet resultSet = offsetQueryPstat.executeQuery();
                        if (resultSet.next()) {
                            long offset = resultSet.getLong("offset");
                            System.out.println("topic: " + partition.topic() + ", partition: " + partition.partition() + ", offset: " + offset);
                            consumer.seek(partition, offset);
                        } else {
                            // 第一次消费数据时，数据库还没有分区记录
                            System.out.println("数据库还没有分区offset记录");
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    String[] fields = record.value().split(",");
                    stuPstat.setInt(1, Integer.parseInt(fields[0]));
                    stuPstat.setString(2, fields[1]);
                    stuPstat.setInt(3, Integer.parseInt(fields[2]));
                    stuPstat.setString(4, fields[3]);

                    stuPstat.execute();

                    // 写入offset
                    offsetPstat.setString(1, String.format("%s:%d", record.topic(), record.partition()));
                    offsetPstat.setLong(2, record.offset() + 1);
                    offsetPstat.setLong(3, record.offset() + 1);

                    offsetPstat.execute();
                    conn.commit();
                } catch (SQLException e) {
                    e.printStackTrace();
                    try {
                        conn.rollback();
                    } catch (SQLException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }

    }
}
