package net.acan.realtime.gmalllogger.controller;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {

    @RequestMapping("/applog")
    public String logger(@RequestParam("param") String logStr){

        // 1. 把数据写入到磁盘
        saveToDisk(logStr);
        //2.将日志数据写入到kafka
        writeToKafka(logStr);

        return "ok";
    }
    @Autowired
    KafkaTemplate<String, String> kafka; //这样才能使用kafka方法
    private void writeToKafka(String logStr) {
        //需要生产者
        kafka.send("ods_log",logStr);
    }

    private void saveToDisk(String logStr) {
        log.info(logStr);
    }
}
