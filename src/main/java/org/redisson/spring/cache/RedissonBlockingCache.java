/**
 * Copyright 2014 Nikita Koksharov, Nickolay Borbit
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.spring.cache;

import org.redisson.RedissonClient;
import org.redisson.core.MessageListener;
import org.redisson.core.RMap;
import org.redisson.core.RMapCache;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class RedissonBlockingCache extends RedissonCache {

    ThreadLocal<Boolean> blockedGet = new ThreadLocal<Boolean>();
    RedissonClient redisson;

    public RedissonBlockingCache(RMapCache<Object, Object> mapCache, CacheConfig config)
    {
        super(mapCache, config);
    }

    public RedissonBlockingCache(RMap<Object, Object> map) {
        this(map, null);
    }

    public RedissonBlockingCache(RMap<Object, Object> map, RedissonClient client) {
        super( map );
        this.redisson = client;
    }

    public RedissonBlockingCache(RMapCache<Object, Object> mapCache, CacheConfig config, RedissonClient client) {

        this(mapCache, config);

        this.redisson = client;
    }


    @Override
    public ValueWrapper get(Object key) {
        Object value = map.get(key);
        if (value == null) {
            if (redisson.getBucket("redisson_lock_get__{" + getName() + '_' + key  +"}").trySet(true)) {
                blockedGet.set(true);
                return null;
            } else {
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicReference<Object> valueRef = new AtomicReference<Object>();
                redisson.getTopic("redisson_release_get__{" + getName()+'_'+ key + "}").addListener(new MessageListener<Object>() {
                    @Override
                    public void onMessage(String channel, Object value) {
                        valueRef.set(value);
                        latch.countDown();
                    }
                });
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                value = valueRef.get();
            }
        }

        return toValueWrapper(value);
    }

    @Override
    public void put(Object key, Object value) {
        if (mapCache != null) {
            mapCache.fastPut(key, value, config.getTTL(), TimeUnit.MILLISECONDS, config.getMaxIdleTime(), TimeUnit.MILLISECONDS);
        } else {
            map.fastPut(key, value);
        }
        if (blockedGet.get() != null) {
            redisson.getTopic("redisson_release_get__{" + getName()+'_'+ key + "}").publish(value);
            redisson.getBucket("redisson_lock_get__{" + getName()+'_'+ key + "}").delete();
            blockedGet.remove();
        }
    }
}
