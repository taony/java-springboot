package com.taony.springboot.redis.controller;

import com.taony.springboot.redis.utils.RedisStreamUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@RestController
@RequestMapping("/test")
public class TestController {

    @Value("${redis-stream.names}")
    private String[]redisStreamNames;

    private final RedisStreamUtil redisUtil;

    @Autowired
    public TestController(RedisStreamUtil redisUtil){
        this.redisUtil = redisUtil;
    }

    @GetMapping("/sendTest/{streamName}")
    public String addStream(@PathVariable String streamName){
        Map<String,Object> message = new HashMap<>();
        message.put("test","hello redismq");
        message.put("send time", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        return redisUtil.addStream(streamName, message).getValue();
    }

    @GetMapping("/getStream")
    public List<MapRecord<String,Object,Object>> getStream(String key){
        return redisUtil.getAllStream(key);
    }


    @GetMapping("/groupRead")
    public void getStreamByGroup(String key, String groupName, String consumerName){
        redisUtil.getStreamByGroup(key,groupName,consumerName);
    }

    @GetMapping("/moreTest/{count}")
    public void moreAddTest(@PathVariable("count") Integer count){
        for(int i=0; i<count; i++){
            Map<String,Object> message1 = new HashMap<>();
            message1.put("name","mystream1");
            message1.put("send time", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            Map<String,Object> message2 = new HashMap<>();
            message2.put("name","mystream2");
            message2.put("send time", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            redisUtil.addStream(redisStreamNames[0],message1);
            redisUtil.addStream(redisStreamNames[1],message2);
        }
    }
}
