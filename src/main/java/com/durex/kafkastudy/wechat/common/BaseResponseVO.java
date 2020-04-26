package com.durex.kafkastudy.wechat.common;

import lombok.Data;

import java.util.UUID;

/**
 * @author : jiangzh
 * @program : com.example.wechatdemo.common
 * @description : 公共返回对象
 * @date : 2020-03-31 13:46
 **/
@Data
public class BaseResponseVO<M> {

    private String requestId;
    private M result;

    public static <M> BaseResponseVO success() {
        BaseResponseVO<M> baseResponseVO = new BaseResponseVO<>();
        baseResponseVO.setRequestId(genRequestId());
        return baseResponseVO;
    }

    public static <M> BaseResponseVO success(M result) {
        BaseResponseVO<M> baseResponseVO = new BaseResponseVO<>();
        baseResponseVO.setRequestId(genRequestId());
        baseResponseVO.setResult(result);
        return baseResponseVO;
    }

    private static String genRequestId() {
        return UUID.randomUUID().toString();
    }

}
