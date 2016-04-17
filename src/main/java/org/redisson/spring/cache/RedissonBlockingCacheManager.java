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
import org.redisson.client.codec.Codec;
import org.redisson.core.RMap;
import org.redisson.core.RMapCache;
import org.springframework.cache.Cache;

import java.util.Map;


public class RedissonBlockingCacheManager extends RedissonSpringCacheManager {


    public RedissonBlockingCacheManager() {
        super();
    }


    public RedissonBlockingCacheManager(RedissonClient redisson, Map<String, CacheConfig> config) {
        this(redisson, config, null);
    }


    public RedissonBlockingCacheManager(RedissonClient redisson, Map<String, CacheConfig> config, Codec codec) {
        this.redisson = redisson;
        this.configMap = config;
        this.codec = codec;
    }

    @Override
    public Cache getCache(String name) {
        CacheConfig config = configMap.get(name);
        if (config == null) {
            config = new CacheConfig();
            configMap.put(name, config);

            RMap<Object, Object> map = createMap(name);
            return new RedissonBlockingCache(map);
        }
        if (config.getMaxIdleTime() == 0 && config.getTTL() == 0) {
            RMap<Object, Object> map = createMap(name);
            return new RedissonBlockingCache(map);
        }
        RMapCache<Object, Object> map = createMapCache(name);
        return new RedissonBlockingCache(map, config);
    }
}


