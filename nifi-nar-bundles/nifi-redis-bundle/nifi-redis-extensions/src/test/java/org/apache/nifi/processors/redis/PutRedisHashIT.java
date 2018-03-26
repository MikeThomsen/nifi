/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.redis.RedisConnectionPool;
import org.apache.nifi.redis.service.RedisConnectionPoolService;
import org.apache.nifi.redis.util.RedisUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class PutRedisHashIT {
    ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testStandalone() throws Exception {
        final String attrName = "key.name";
        final String attrValue = "simple_test";
        RedisConnectionPool connectionPool = new RedisConnectionPoolService();
        TestRunner runner = TestRunners.newTestRunner(PutRedisHash.class);

        runner.addControllerService("connPool", connectionPool);
        runner.setProperty(connectionPool, RedisUtils.CONNECTION_STRING, "localhost:6379");
        runner.enableControllerService(connectionPool);
        runner.setProperty(PutRedisHash.REDIS_CONNECTION_POOL, "connPool");
        runner.setProperty(PutRedisHash.NAME_ATTRIBUTE, attrName);
        runner.assertValid();

        Map<String, String> attrs = new HashMap<>();
        attrs.put(attrName, attrValue);
        Map<String, Object> sets = new HashMap<>();
        sets.put("test1", "msg");
        sets.put("test2", "msg");
        sets.put("test3", "msg");
        sets.put("test4", Integer.MAX_VALUE);
        sets.put("test5", Long.MAX_VALUE);

        byte[] json = mapper.writeValueAsBytes(sets);

        runner.enqueue(json, attrs);
        runner.run(1, true, true);

        runner.assertTransferCount(PutRedisHash.REL_FAILURE, 0);
        runner.assertTransferCount(PutRedisHash.REL_SUCCESS, 1);

        Jedis jedis = new Jedis("localhost", 6379);
        Map<String, String> val = jedis.hgetAll(attrValue);
        Assert.assertNotNull(val);
        Assert.assertEquals(sets.size(), val.size());
    }
}
