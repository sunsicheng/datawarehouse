package com.atguigu.gmall.gmalllogger.controller;

import com.atguigu.gmall.gmalllogger.common.Constant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/3/29 21:57
 */

@RestController
public class LoggerController {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @GetMapping("/applog")
    public String get(@RequestParam("param") String log) {
        saveToKafka(Constant.ODS_LOG, log);
        return "本地访问成功";
    }

    public void saveToKafka(String topic, String data) {
        kafkaTemplate.send(topic, data);
    }
}
