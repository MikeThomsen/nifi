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
package org.apache.nifi.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import io.netty.handler.ssl.ClientAuth;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.core.ConsistencyLevel.ALL;
import static com.datastax.oss.driver.api.core.ConsistencyLevel.ANY;
import static com.datastax.oss.driver.api.core.ConsistencyLevel.EACH_QUORUM;
import static com.datastax.oss.driver.api.core.ConsistencyLevel.LOCAL_ONE;
import static com.datastax.oss.driver.api.core.ConsistencyLevel.LOCAL_QUORUM;
import static com.datastax.oss.driver.api.core.ConsistencyLevel.LOCAL_SERIAL;
import static com.datastax.oss.driver.api.core.ConsistencyLevel.ONE;
import static com.datastax.oss.driver.api.core.ConsistencyLevel.QUORUM;
import static com.datastax.oss.driver.api.core.ConsistencyLevel.SERIAL;
import static com.datastax.oss.driver.api.core.ConsistencyLevel.THREE;
import static com.datastax.oss.driver.api.core.ConsistencyLevel.TWO;

public interface CassandraSessionProviderService extends ControllerService {
    /**
     * Obtains a Cassandra session instance
     * @return {@link CqlSession}
     */
    CqlSession getCassandraSession();

    PropertyDescriptor CONTACT_POINTS = new PropertyDescriptor.Builder()
            .name("Cassandra Contact Points")
            .description("Contact points are addresses of Cassandra nodes. The list of contact points should be "
                    + "comma-separated and in hostname:port format. Example node1:port,node2:port,...."
                    + " The default client port for Cassandra is 9042, but the port(s) must be explicitly specified.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
            .build();

    PropertyDescriptor KEYSPACE = new PropertyDescriptor.Builder()
            .name("Keyspace")
            .description("The Cassandra Keyspace to connect to. If no keyspace is specified, the query will need to " +
                    "include the keyspace name before any table reference, in case of 'query' native processors or " +
                    "if the processor supports the 'Table' property, the keyspace name has to be provided with the " +
                    "table name in the form of <KEYSPACE>.<TABLE>")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("Client Auth")
            .description("Client authentication policy when connecting to secure (TLS/SSL) cluster. "
                    + "Possible values are REQUIRED, WANT, NONE. This property is only used when an SSL Context "
                    + "has been defined and enabled.")
            .required(false)
            .allowableValues(ClientAuth.values())
            .defaultValue("REQUIRED")
            .build();

    PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username to access the Cassandra cluster")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password to access the Cassandra cluster")
            .required(false)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    List<ConsistencyLevel> ALL_CONSISTENCY_VALUES = Arrays.asList(ANY, ONE, TWO, THREE, QUORUM, ALL,
            LOCAL_ONE, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL);

    PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor.Builder()
            .name("Consistency Level")
            .description("The strategy for how many replicas must respond before results are returned.")
            .required(true)
            .allowableValues(ALL_CONSISTENCY_VALUES.stream().map(ConsistencyLevel::name).collect(Collectors.toSet()))
            .defaultValue("ONE")
            .build();
//
//    PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
//            .name("Compression Type")
//            .description("Enable compression at transport-level requests and responses")
//            .required(false)
//            .allowableValues(ProtocolOptions.Compression.values())
//            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//            .defaultValue("NONE")
//            .build();

    PropertyDescriptor READ_TIMEOUT_MS = new PropertyDescriptor.Builder()
            .name("read-timeout-ms")
            .displayName("Read Timout (ms)")
            .description("Read timeout (in milliseconds). 0 means no timeout. If no value is set, the underlying default will be used.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    PropertyDescriptor CONNECT_TIMEOUT_MS = new PropertyDescriptor.Builder()
            .name("connect-timeout-ms")
            .displayName("Connect Timeout (ms)")
            .description("Connection timeout (in milliseconds). 0 means no timeout. If no value is set, the underlying default will be used.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
}
