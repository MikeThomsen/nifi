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

import com.datastax.oss.driver.api.core.CqlSession;
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
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

@Tags({"cassandra", "dbcp", "database", "connection", "pooling"})
@CapabilityDescription("Provides connection session for Cassandra processors to work with Apache Cassandra.")
public class ScyllaDBSessionProvider extends AbstractControllerService implements CassandraSessionProviderService {

    public static final int DEFAULT_CASSANDRA_PORT = 9042;

    // Common descriptors

    private List<PropertyDescriptor> properties;
    private CqlSession cassandraSession;

    @Override
    public void init(final ControllerServiceInitializationContext context) {
        List<PropertyDescriptor> props = new ArrayList<>();

        props.add(CONTACT_POINTS);
        props.add(CLIENT_AUTH);
        //TODO: replace
//        props.add(CONSISTENCY_LEVEL);
//        props.add(COMPRESSION_TYPE);
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
    }

    @Override
    public CqlSession getCassandraSession() {
        if (cassandraSession != null) {
            return cassandraSession;
        } else {
            throw new ProcessException("Unable to get the Cassandra session.");
        }
    }

    private void connectToCassandra(ConfigurationContext context) {
        if (cassandraSession == null) {
            final String contactPointList = context.getProperty(CONTACT_POINTS).evaluateAttributeExpressions().getValue();

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

            CqlSession cqlSession = CqlSession.builder()
                    .addContactPoints(contactPoints)
                    .withAuthCredentials(username, password)
                    .withSslContext(sslContext)
                    .build();

            // Create the cluster and connect to it
            cassandraSession = cqlSession;
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
}
