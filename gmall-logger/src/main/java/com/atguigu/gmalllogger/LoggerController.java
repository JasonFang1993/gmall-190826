package com.atguigu.gmalllogger;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


import com.atguigu.constants.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
//@Controller
@Slf4j
public class LoggerController {

    //自动注入
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("test1")
    public String test1() {
        return "success";
    }

    @GetMapping("test2")
    public String test2(@RequestParam("a") String aa) {
        return aa;
    }

    @PostMapping("log")
    //ResponseBody
    public String logger(@RequestParam("logString") String logStr) {

        //添加事件数字段落
        JSONObject jsonObject = JSON.parseObject(logStr);
        jsonObject.put("ts", System.currentTimeMillis());
        String tsJson = jsonObject.toString();


        //使用log4j打印日志到控制台文件
        log.info(tsJson);

        //使用kafka生产者将数据发送到kafka集群
        if (tsJson.contains("startup")) {
            //将数据发到启动日志主题
            kafkaTemplate.send(GmallConstants.GMALL_STARTUP_TOPIC, tsJson);
        } else {
            //将数据发送至事件日志主题
            kafkaTemplate.send(GmallConstants.GMALL_EVENT_TOPIC, tsJson);
        }

        return "success";
    }
}
