package com.durex.kafkastudy.wechat.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.durex.kafkastudy.wechat.config.WechatTemplateProperties;
import com.durex.kafkastudy.wechat.utils.FileUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * @author gelong
 * @date 2020/4/16 20:52
 */
@Slf4j
@Service
public class WechatTemplateServiceImpl implements WechatTemplateService {

    private WechatTemplateProperties wechatTemplateConfig;
    private final KafkaProducer<String, Object> kafkaProducer;

    @Autowired
    public WechatTemplateServiceImpl(WechatTemplateProperties wechatTemplateConfig, KafkaProducer<String, Object> kafkaProducer) {
        this.wechatTemplateConfig = wechatTemplateConfig;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public WechatTemplateProperties.WechatTemplate getWechatTemplate() {
        List<WechatTemplateProperties.WechatTemplate> templates = wechatTemplateConfig.getTemplates();
        Optional<WechatTemplateProperties.WechatTemplate> wechatTemplate = templates.stream().
                filter(WechatTemplateProperties.WechatTemplate::isActive).findFirst();
        return wechatTemplate.orElse(new WechatTemplateProperties.WechatTemplate());
    }

    @Override
    public void templateReported(JSONObject reportInfo) {
        // kafka
        log.info("templateReported: [{}]", reportInfo);
        String topicName = "durex";
        String templateId = reportInfo.getString("templateId");
        JSONArray reportData = reportInfo.getJSONArray("result");
        ProducerRecord<String, Object> record = new ProducerRecord<>(topicName, templateId , reportData);
        kafkaProducer.send(record);
    }

    @Override
    public JSONObject templateStatistics(String templateId) {
        // 判断结果类型
        if (wechatTemplateConfig.getTemplateResultType() == 0) {
            return FileUtils.
                    readFile2JsonObject(wechatTemplateConfig.getTemplateResultFilePath()).
                    orElse(new JSONObject());
        }
        return new JSONObject();
    }
}
