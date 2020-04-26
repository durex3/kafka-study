package com.durex.kafkastudy.wechat.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author gelong
 * @date 2020/4/21 23:16
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "wechat.kafka")
public class KafkaProperties {

    private String bootstrapServersConfig;
    private String acksConfig;
    private String retriesConfig;
    private String batchSizeConfig;
    private String lingerMsConfig;
    private String bufferMemoryConfig;
    private String keySerializerClassConfig;
    private String valueSerializerClassConfig;
}
