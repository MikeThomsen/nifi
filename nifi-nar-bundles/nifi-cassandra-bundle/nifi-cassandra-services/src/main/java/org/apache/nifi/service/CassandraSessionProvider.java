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
package org.apache.nifi.service;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import javax.net.ssl.SSLContext;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.cassandra.CassandraSessionProviderService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.ssl.SSLContextService;

@Tags({"cassandra", "dbcp", "database", "connection", "pooling"})
@CapabilityDescription("Provides connection session for Cassandra processors to work with Apache Cassandra.")
public class CassandraSessionProvider extends AbstractControllerService implements CassandraSessionProviderService {

    public static final int DEFAULT_CASSANDRA_PORT = 9042;

    // Common descriptors

    private List<PropertyDescriptor> properties;
    private Cluster cluster;
    private Session cassandraSession;

    @Override
    public void init(final ControllerServiceInitializationContext context) {
        List<PropertyDescriptor> props = new ArrayList<>();

        props.add(CONTACT_POINTS);
        props.add(CLIENT_AUTH);
        props.add(CONSISTENCY_LEVEL);
        props.add(COMPRESSION_TYPE);
        props.add(KEYSPACE);
        props.add(USERNAME);
        props.add(PASSWORD);
        props.add(PROP_SSL_CONTEXT_SERVICE);
        props.add(READ_TIMEOUT_MS);
        props.add(CONNECT_TIMEOUT_MS);

        properties = props;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        connectToCassandra(context);
    }

    @OnDisabled
    public void onDisabled(){
        if (cassandraSession != null) {
            cassandraSession.close();
            cassandraSession = null;
        }
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
    }

    @Override
    public Cluster getCluster() {
        if (cluster != null) {
            return cluster;
        } else {
            throw new ProcessException("Unable to get the Cassandra cluster detail.");
        }
    }

    @Override
    public Session getCassandraSession() {
        if (cassandraSession != null) {
            return cassandraSession;
        } else {
            throw new ProcessException("Unable to get the Cassandra session.");
        }
    }

    private void connectToCassandra(ConfigurationContext context) {
        if (cluster == null) {
            ComponentLog log = getLogger();
            final String contactPointList = context.getProperty(CONTACT_POINTS).evaluateAttributeExpressions().getValue();
            final String consistencyLevel = context.getProperty(CONSISTENCY_LEVEL).getValue();
            final String compressionType = context.getProperty(COMPRESSION_TYPE).getValue();

            List<InetSocketAddress> contactPoints = getContactPoints(contactPointList);

            // Set up the client for secure (SSL/TLS communications) if configured to do so
            final SSLContextService sslService =
                    context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
            final SSLContext sslContext;

            if (sslService == null) {
                sslContext = null;
            } else {
                sslContext = sslService.createContext();;
            }

            final String username, password;
            PropertyValue usernameProperty = context.getProperty(USERNAME).evaluateAttributeExpressions();
            PropertyValue passwordProperty = context.getProperty(PASSWORD).evaluateAttributeExpressions();

            if (usernameProperty != null && passwordProperty != null) {
                username = usernameProperty.getValue();
                password = passwordProperty.getValue();
            } else {
                username = null;
                password = null;
            }

            final Integer readTimeoutMillis = context.getProperty(READ_TIMEOUT_MS).evaluateAttributeExpressions().asInteger();
            final Integer connectTimeoutMillis = context.getProperty(CONNECT_TIMEOUT_MS).evaluateAttributeExpressions().asInteger();

            // Create the cluster and connect to it
            Cluster newCluster = createCluster(contactPoints, sslContext, username, password, compressionType, readTimeoutMillis, connectTimeoutMillis);
            PropertyValue keyspaceProperty = context.getProperty(KEYSPACE).evaluateAttributeExpressions();
            final Session newSession;
            if (keyspaceProperty != null) {
                newSession = newCluster.connect(keyspaceProperty.getValue());
            } else {
                newSession = newCluster.connect();
            }
            newCluster.getConfiguration().getQueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel));
            Metadata metadata = newCluster.getMetadata();
            log.info("Connected to Cassandra cluster: {}", new Object[]{metadata.getClusterName()});

            cluster = newCluster;
            cassandraSession = newSession;
        }
    }

    private List<InetSocketAddress> getContactPoints(String contactPointList) {

        if (contactPointList == null) {
            return null;
        }

        final String[] contactPointStringList = contactPointList.split(",");
        List<InetSocketAddress> contactPoints = new ArrayList<>();

        for (String contactPointEntry : contactPointStringList) {
            String[] addresses = contactPointEntry.split(":");
            final String hostName = addresses[0].trim();
            final int port = (addresses.length > 1) ? Integer.parseInt(addresses[1].trim()) : DEFAULT_CASSANDRA_PORT;

            contactPoints.add(new InetSocketAddress(hostName, port));
        }

        return contactPoints;
    }

    private Cluster createCluster(final List<InetSocketAddress> contactPoints, final SSLContext sslContext,
                                  final String username, final String password, final String compressionType,
                                  final Integer readTimeoutMillis, final Integer connectTimeoutMillis) {

        Cluster.Builder builder = Cluster.builder().addContactPointsWithPorts(contactPoints);
        if (sslContext != null) {
            final SSLOptions sslOptions = RemoteEndpointAwareJdkSSLOptions.builder()
                    .withSSLContext(sslContext)
                    .build();
            builder = builder.withSSL(sslOptions);
        }

        if (username != null && password != null) {
            builder = builder.withCredentials(username, password);
        }

        if (ProtocolOptions.Compression.SNAPPY.name().equals(compressionType)) {
            builder = builder.withCompression(ProtocolOptions.Compression.SNAPPY);
        } else if (ProtocolOptions.Compression.LZ4.name().equals(compressionType)) {
            builder = builder.withCompression(ProtocolOptions.Compression.LZ4);
        }

        SocketOptions socketOptions = new SocketOptions();
        if (readTimeoutMillis != null) {
            socketOptions.setReadTimeoutMillis(readTimeoutMillis);
        }
        if (connectTimeoutMillis != null) {
            socketOptions.setConnectTimeoutMillis(connectTimeoutMillis);
        }

        builder.withSocketOptions(socketOptions);

        return builder.build();
    }
}
