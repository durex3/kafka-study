package com.durex.kafkastudy.wechat.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

/**
 * @author gelong
 * @date 2020/4/21 23:13
 */
@Configuration
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;

    @Autowired
    public KafkaConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public KafkaProducer<String, Object> kafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServersConfig());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, kafkaProperties.getAcksConfig());
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, kafkaProperties.getRetriesConfig());
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, kafkaProperties.getBatchSizeConfig());
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, kafkaProperties.getLingerMsConfig());
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, kafkaProperties.getBufferMemoryConfig());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                kafkaProperties.getKeySerializerClassConfig());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                kafkaProperties.getValueSerializerClassConfig());
        return new KafkaProducer<>(properties);
    }
}
