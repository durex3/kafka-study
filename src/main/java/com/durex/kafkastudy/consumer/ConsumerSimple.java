package com.durex.kafkastudy.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * @author gelong
 * @date 2020/4/26 22:14
 */
public class ConsumerSimple {

    private static final String TOPIC_NAME = "durex";

    public static void hello() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.1.104:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n",
                        record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

    /**
     * 手动提交消费的消息
     */
    public static void commitOffset() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.1.104:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n",
                        record.partition(), record.offset(), record.key(), record.value());
            }
            consumer.commitAsync();
        }
    }

    /**
     * 手动提交消费的消息 每个partition单独处理
     */
    public static void commitPartitionOffset() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.1.104:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            // 每个partition单独处理
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> consumerRecords = records.records(partition);
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                }
                // 这一次消费的终点
                long lastOffset = consumerRecords.get(consumerRecords.size() - 1).offset();
                Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                map.put(partition, new OffsetAndMetadata(lastOffset + 1));
                consumer.commitSync(map);
            }
        }
    }

    /**
     * 执行消费某些partition
     */
    public static void commitPartition2Offset() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.1.104:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);
        // 指定消费的partition
        consumer.assign(Collections.singletonList(p0));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            // 每个partition单独处理
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> consumerRecords = records.records(partition);
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                }
                // 这一次消费的终点
                long lastOffset = consumerRecords.get(consumerRecords.size() - 1).offset();
                Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                map.put(partition, new OffsetAndMetadata(lastOffset + 1));
                consumer.commitSync(map);
            }
        }
    }

    /**
     * 控制offset的起始位置
     */
    public static void controlOffset() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.1.104:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);
        // 指定消费的partition
        consumer.assign(Collections.singletonList(p0));
        while (true) {
            consumer.seek(p0, 468);
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            // 每个partition单独处理
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> consumerRecords = records.records(partition);
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                }
                // 这一次消费的终点
                long lastOffset = consumerRecords.get(consumerRecords.size() - 1).offset();
                Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                map.put(partition, new OffsetAndMetadata(lastOffset + 1));
                consumer.commitSync(map);
            }
        }
    }

    /**
     * 流量控制 - 限流
     */
    private static void controlPause() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.1.104:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);
        TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);

        // 消费订阅某个Topic的某个分区
        consumer.assign(Arrays.asList(p0, p1));
        long totalNum = 40;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            // 每个partition单独处理
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> pRecord = records.records(partition);
                long num = 0;
                for (ConsumerRecord<String, String> record : pRecord) {
                    System.out.printf("patition = %d , offset = %d, key = %s, value = %s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                    /*
                        1、接收到record信息以后，去令牌桶中拿取令牌
                        2、如果获取到令牌，则继续业务处理
                        3、如果获取不到令牌， 则pause等待令牌
                        4、当令牌桶中的令牌足够， 则将consumer置为resume状态
                     */
                    num++;
                    if (record.partition() == 0) {
                        if (num >= totalNum) {
                            consumer.pause(Arrays.asList(p0));
                        }
                    }

                    if (record.partition() == 1) {
                        if (num == 40) {
                            consumer.resume(Arrays.asList(p0));
                        }
                    }
                }

                long lastOffset = pRecord.get(pRecord.size() - 1).offset();
                // 单个partition中的offset，并且进行提交
                Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
                offset.put(partition, new OffsetAndMetadata(lastOffset + 1));
                // 提交offset
                consumer.commitSync(offset);
                System.out.println("=============partition - " + partition + " end================");
            }
        }
    }

    public static void main(String[] args) {
        controlPause();
    }
}
