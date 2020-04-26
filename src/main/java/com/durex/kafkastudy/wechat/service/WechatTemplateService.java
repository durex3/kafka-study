package com.durex.kafkastudy.wechat.service;

import com.alibaba.fastjson.JSONObject;
import com.durex.kafkastudy.wechat.config.WechatTemplateProperties;

/**
 * @author gelong
 * @date 2020/4/16 20:52
 */
public interface WechatTemplateService {

    /**
     * 获取调查问卷查询模板
     * @return WechatTemplateConfig.WechatTemplate
     */
    WechatTemplateProperties.WechatTemplate getWechatTemplate();

    /**
     *  上报调查问卷结果
     * @param reportInfo 问卷信息
     */
    void templateReported(JSONObject reportInfo);

    /**
     * 获取调查问卷的统计结果
     * @param templateId 模板id
     * @return JSONObject
     */
    JSONObject templateStatistics(String templateId);

}
