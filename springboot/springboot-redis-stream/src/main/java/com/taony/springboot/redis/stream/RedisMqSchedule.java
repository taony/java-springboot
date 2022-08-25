/**
 * Copyright(c) 2022 dev.sino-essence.cn All rights reserved.
 */
package com.taony.springboot.redis.stream;

import cn.hutool.core.util.IdUtil;
import com.taony.springboot.redis.utils.RedisStreamUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * RedisMqSchedule TODO
 *
 * @author: taony
 * @email:taonyzhang@gmail.com
 * @date: created on 2022-08-25 10:20
 */

@Component
@EnableScheduling
public class RedisMqSchedule {

    @Autowired
    RedisStreamUtil redisUtil;

    @Scheduled(fixedRate = 3000)
    private void addStreamTask() {
        Map<String, Object> msg = new HashMap<>();
        msg.put("reportId", IdUtil.getSnowflakeNextIdStr());
        msg.put("entName", "企业-" + IdUtil.nanoId());
        redisUtil.addStream("es-ztc-batch-mq1", msg);
        System.out.println("添加任务" + System.nanoTime());
    }

}