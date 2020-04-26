package com.durex.kafkastudy.wechat.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * @author gelong
 * @date 2020/4/16 21:19
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "template")
public class WechatTemplateProperties {

    private List<WechatTemplate> templates;
    private int templateResultType;
    private String templateResultFilePath;

    @Data
    public static class WechatTemplate {
        private String templateId;
        private String templateFilePath;
        private boolean active;
    }
}
