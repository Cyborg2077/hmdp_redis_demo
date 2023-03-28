package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    @Override
    public Result queryById(Long id) {
        Shop shop = queryWithMutex(id);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    @Override
    public Result update(Shop shop) {
        //1. 先判空
        if (shop.getId() == null) {
            return Result.fail("店铺ID不能为空");
        }
        //2. 更新数据
        this.updateById(shop);
        //3. 删除缓存，等下次查询数据的时候，再将其存入redis
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        return Result.ok(shop);
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    @Override
    public Shop queryWithPassThrough(Long id) {
        String key = CACHE_SHOP_KEY + id;
        //1. 先从缓存中查，
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2. 缓存中有，则直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //3. 缓存中没有，从数据库查
        Shop shop = getById(id);
        //4. 数据库也没有，那就真没有了，但是要缓存空对象，防止缓存穿透
        if (shop == null) {
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.SECONDS);
            return null;
        }
        //5. 如果数据库里有，将其转为json字符串
        shopJson = JSONUtil.toJsonStr(shop);
        //6. 然后存入redis中，并设置TTL
        stringRedisTemplate.opsForValue().set(key, String.valueOf(shopJson), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //7. 返回结果给前端
        return shop;
    }


    /**
     * 互斥锁解决缓存击穿
     *
     * @param id 商铺id
     * @return 根据商铺id返回的Shop对象
     */
    @Override
    public Shop queryWithMutex(Long id) {
        String key = CACHE_SHOP_KEY + id;
        //1. 先从缓存中查，
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2. 缓存中有，则直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //3. 缓存中没有，从数据库查
        Shop shop;
        try {
            boolean flag = tryLock(LOCK_SHOP_KEY + id);
            if (!flag) {
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            shop = getById(id);
            //4. 数据库也没有，那就真没有了，但是要缓存空对象，防止缓存穿透
            if (shop == null) {
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.SECONDS);
                return null;
            }
            //5. 如果数据库里有，将其转为json字符串
            shopJson = JSONUtil.toJsonStr(shop);
            //6. 然后存入redis中，并设置TTL
            stringRedisTemplate.opsForValue().set(key, String.valueOf(shopJson), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unlock(LOCK_SHOP_KEY + id);
        }
        //7. 返回结果给前端
        return shop;
    }

    @Override
    public Shop queryWithLogicalExpire(Long id) {
        //1. 先从redis中查询
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        //2. 没查到，将json转为Bean返回
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }
        //3. 查到了，判断缓存是否过期
        //3.1 获取redisData，其中包含了数据和过期时间
        RedisData redisData = BeanUtil.toBean(shopJson, RedisData.class);
        //3.2 取出其中的商铺数据
        JSONObject data = (JSONObject) redisData.getData();
        //3.2.1 将商铺数据转为Shop对象
        Shop shop = BeanUtil.toBean(data, Shop.class);
        //3.3 获取过期时间
        LocalDateTime time = redisData.getExpireTime();
        //4. 如果未过期，直接返回shop
        if (LocalDateTime.now().isBefore(time)) {
            return shop;
        }
        //5. 如果过期，获取互斥锁
        boolean flag = tryLock(LOCK_SHOP_KEY + id);
        //6. 拿到了锁
        if (flag) {
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    this.saveShop2Redis(id, CACHE_SHOP_TTL);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(LOCK_SHOP_KEY + id);
                }
            });
        }
        //7. 未拿到锁，返回旧数据
        return shop;
    }

    private void saveShop2Redis(Long id, Long ttl) {
        Shop shop = this.getById(id);
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(ttl));
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

}
