package com.taony.springboot.redis.stream;

import com.taony.springboot.redis.utils.RedisStreamUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.stream.StreamListener;


@Slf4j
public class RedisStreamListener implements StreamListener<String, MapRecord<String, String, String>> {

    RedisStreamUtil redisUtil;

    public RedisStreamListener(RedisStreamUtil redisUtil) {
        this.redisUtil = redisUtil;
    }

    @Override
    public void onMessage(MapRecord<String, String, String> entries) {
        try {
            Thread.sleep(5000);
            log.info(entries.toString());
            redisUtil.delField(entries.getStream(), entries.getId().getValue());
        } catch (Exception e) {
            log.error("error message:{}", e.getMessage());
        }
    }

}
