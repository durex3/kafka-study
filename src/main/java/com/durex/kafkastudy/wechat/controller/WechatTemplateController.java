package com.durex.kafkastudy.wechat.controller;

import com.alibaba.fastjson.JSON;
import com.durex.kafkastudy.wechat.common.BaseResponseVO;
import com.durex.kafkastudy.wechat.config.WechatTemplateProperties;
import com.durex.kafkastudy.wechat.service.WechatTemplateService;
import com.durex.kafkastudy.wechat.utils.FileUtils;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * @author gelong
 * @date 2020/4/16 20:49
 */
@RequestMapping("/v1")
@RestController
public class WechatTemplateController {

    private final WechatTemplateService wechatTemplateService;

    @Autowired
    public WechatTemplateController(WechatTemplateService wechatTemplateService) {
        this.wechatTemplateService = wechatTemplateService;
    }

    @GetMapping("/template")
    public BaseResponseVO getTemplate() {
        WechatTemplateProperties.WechatTemplate wechatTemplate = wechatTemplateService.getWechatTemplate();
        Map<String, Object> map = Maps.newHashMap();
        map.put("templateId", wechatTemplate.getTemplateId());
        map.put("template", FileUtils.readFile2JsonArray(wechatTemplate.getTemplateFilePath()));
        return BaseResponseVO.success(map);
    }

    @GetMapping("/template/result")
    public BaseResponseVO templateStatistics(@RequestParam(value = "templateId",
            required = false) String templateId) {
        return BaseResponseVO.success(wechatTemplateService.templateStatistics(templateId));
    }

    @PostMapping("/template/report")
    public BaseResponseVO dataReported(@RequestBody String reportedData) {
        wechatTemplateService.templateReported(JSON.parseObject(reportedData));
        return BaseResponseVO.success();
    }
}
