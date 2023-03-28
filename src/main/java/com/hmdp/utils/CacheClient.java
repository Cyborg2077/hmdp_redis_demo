package com.hmdp.utils;


import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;

@Slf4j
@Component
public class CacheClient {
    private StringRedisTemplate stringRedisTemplate;
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(100);

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 将任意Java对象序列化为JSON，并存储到String类型的Key中，并可以设置TTL过期时间
     *
     * @param key      key名
     * @param value    Java对象
     * @param time     TTL
     * @param timeUnit TTL时间单位
     */
    public void set(String key, Object value, Long time, TimeUnit timeUnit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, timeUnit);
    }

    public void setWithLogicExpire(String key, Object value, Long time, TimeUnit timeUnit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit timeUnit) {
        String key = keyPrefix + id;
        String dataJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(dataJson)) {
            return JSONUtil.toBean(dataJson, type);
        }
        if (dataJson != null) {
            return null;
        }
        R r = dbFallback.apply(id);
        if (r == null) {
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.SECONDS);
            return null;
        }
        String jsonStr = JSONUtil.toJsonStr(r);
        this.set(key, jsonStr, time, timeUnit);
        return r;
    }

    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit timeUnit) {
        //1. 拼key
        String key = keyPrefix + id;
        //2. 去redis中查询数据
        String dataJson = stringRedisTemplate.opsForValue().get(key);
        //3. 没查到则返回null
        if (StrUtil.isBlank(dataJson)) {
            return null;
        }
        //4. 查到了，则转为RedisData类型数据
        RedisData redisData = JSONUtil.toBean(dataJson, RedisData.class);
        //5. 获取data，转化为R对象
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        //6. 获取过期时间
        LocalDateTime expireTime = redisData.getExpireTime();
        //7. 如果过期时间没到，则直接返回r对象
        if (LocalDateTime.now().isBefore(expireTime)) {
            return r;
        }
        //8. 如果过期时间到了，新建一个线程来异步更新数据
        boolean flag = tryLock(key);
        if (flag) {
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //9. 从数据库中查询数据
                    R tmp = dbFallback.apply(id);
                    //10. 将其存入redis
                    this.setWithLogicExpire(key, tmp, time, timeUnit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(key);
                }
            });
            //11. 也是直接返回r
            return r;
        }
        //12. 没拿到锁，也直接返回
        return r;
    }

    public <R, ID> R queryWithMutex(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit timeUnit) {
        //1. 拼完整的key
        String key = keyPrefix + id;
        //2. 从redis中查询
        String json = stringRedisTemplate.opsForValue().get(key);
        //3. 查到了，返回数据
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }
        //4. 没查到，去数据库中查询
        R r = null;
        try {
            boolean flag = tryLock(LOCK_SHOP_KEY + id);
            if (!flag) {
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, id, type, dbFallback, time, timeUnit);
            }
            r = dbFallback.apply(id);
            if (r == null) {
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            this.set(key, r, time, timeUnit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unlock(LOCK_SHOP_KEY + id);
        }
        return r;
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }


}
