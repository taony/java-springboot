package com.taony.springboot.redis.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;


/**
 * Redis S
 * @author taony
 */
@Slf4j
@Component
public class RedisStreamUtil {
    private final RedisTemplate<String, String> redisTemplate;

    @Autowired
    public RedisStreamUtil(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }


    /**
     * 添加stream消息
     *
     * @param key
     * @param message
     * @return
     */
    public RecordId addStream(String key, Map<String, Object> message) {
        return redisTemplate.opsForStream().add(key, message);
    }

    /**
     * 添加分组
     *
     * @param key
     * @param groupName
     */
    public void addGroup(String key, String groupName) {
        redisTemplate.opsForStream().createGroup(key, groupName);
    }

    /**
     * 删除消息
     *
     * @param key
     * @param fieldId
     */
    public void delField(String key, String fieldId) {
        redisTemplate.opsForStream().delete(key, fieldId);
    }

    /**
     * 读取所有stream消息
     */
    public List<MapRecord<String, Object, Object>> getAllStream(String key) {
        List<MapRecord<String, Object, Object>> range = redisTemplate.opsForStream().range(key, Range.open("-", "+"));
        if (range == null) {
            return null;
        }
        for (MapRecord<String, Object, Object> mapRecord : range) {
            redisTemplate.opsForStream().delete(key, mapRecord.getId());
        }
        return range;
    }

    /**
     * 获取消息
     *
     * @param key
     */
    public void getStream(String key) {
        List<MapRecord<String, Object, Object>> read = redisTemplate.opsForStream()
                .read(StreamReadOptions.empty().block(Duration.ofMillis(1000 * 30)).count(2), StreamOffset.latest(key));
        System.out.println(read);
    }

    /**
     * 根据分组获取消息
     *
     * @param key
     * @param groupName
     * @param consumerName
     */
    public void getStreamByGroup(String key, String groupName, String consumerName) {
        List<MapRecord<String, Object, Object>> read = redisTemplate.opsForStream().read(Consumer.from(groupName, consumerName), StreamReadOptions.empty(), StreamOffset.create(key, ReadOffset.lastConsumed()));
        log.info("group read :{}", read);
    }

    /**
     * 验证是否存在Key
     *
     * @param key
     * @return
     */
    public boolean hasKey(String key) {
        return redisTemplate.hasKey(key) != null;
    }

}
