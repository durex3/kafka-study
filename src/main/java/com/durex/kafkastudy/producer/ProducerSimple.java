package com.durex.kafkastudy.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @author gelong
 * @date 2020/4/15 21:13
 */
public class ProducerSimple {

    private static final String TOPIC_NAME = "durex";
    private static Properties properties = new Properties();
    private static final int SIZE = 10;

    static {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.104:9092");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "0");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
    }

    /**
     * producer异步发送
     */
    public static void producerSend() {
        Producer<String, String> producer = new KafkaProducer<>(properties);
        // 消息对象
        for (int i = 0; i < SIZE * 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key-" + i , "value-" + i);
            producer.send(record);
        }
        producer.close();
    }

    /**
     * producer异步阻塞发送
     */
    public static void producerSyncSend() throws Exception {
        Producer<String, String> producer = new KafkaProducer<>(properties);
        // 消息对象
        for (int i = 0; i < SIZE; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key-" + i , "value-" + i);
            Future<RecordMetadata> send = producer.send(record);
            RecordMetadata recordMetadata = send.get();
            System.out.println(recordMetadata);
        }
        producer.close();
    }

    /**
     * producer异步回调发送
     */
    public static void producerCallbackSend() throws Exception {
        Producer<String, String> producer = new KafkaProducer<>(properties);
        // 消息对象
        for (int i = 0; i < SIZE; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key-" + i , "value-" + i);
            producer.send(record, (recordMetadata, e) ->
                    System.out.println("recordMetadata: " + recordMetadata + "," + "exception: " + e));
        }
        producer.close();
    }

    /**
     * producer异步回调发送
     */
    public static void producerCallbackAndPartitionSend() throws Exception {
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                "com.durex.kafkastudy.producer.PartitionSimple");
        Producer<String, String> producer = new KafkaProducer<>(properties);
        // 消息对象
        for (int i = 0; i < SIZE; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key-" + i , "value-" + i);
            producer.send(record, (recordMetadata, e) ->
                    System.out.println("partition: " + recordMetadata.partition() + "," + "exception: " + e));
        }
        producer.close();
    }

    public static void main(String[] args) throws Exception {
        producerSend();
    }
}
