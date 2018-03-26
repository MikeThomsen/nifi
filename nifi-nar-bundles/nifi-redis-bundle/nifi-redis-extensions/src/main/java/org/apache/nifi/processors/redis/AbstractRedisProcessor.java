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

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.redis.util.RedisAction;
import org.apache.nifi.redis.util.RedisUtils;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

import java.io.IOException;

public abstract class AbstractRedisProcessor extends AbstractProcessor {
    private volatile JedisConnectionFactory connectionFactory;
    private ProcessContext context;

    @OnScheduled
    public void init(ProcessContext context) {
        this.context = context;
    }

    synchronized RedisConnection getRedis() {
        if (connectionFactory == null) {
            connectionFactory = RedisUtils.createConnectionFactory(context, getLogger());
        }

        return connectionFactory.getConnection();
    }

    protected <T> T withConnection(final RedisAction<T> action) throws IOException {
        RedisConnection redisConnection = null;
        try {
            redisConnection = getRedis();
            return action.execute(redisConnection);
        } finally {
            if (redisConnection != null) {
                try {
                    redisConnection.close();
                } catch (Exception e) {
                    getLogger().warn("Error closing connection: " + e.getMessage(), e);
                }
            }
        }
    }
}
