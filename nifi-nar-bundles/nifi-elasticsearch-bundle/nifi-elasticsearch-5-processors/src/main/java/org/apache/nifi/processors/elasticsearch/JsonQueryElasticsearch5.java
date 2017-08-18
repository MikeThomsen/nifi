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
package org.apache.nifi.processors.elasticsearch;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@SupportsBatching
@Tags({"elasticsearch", "elasticsearch 5", "fetch", "read", "get"})
@CapabilityDescription("Retrieves a document from Elasticsearch using the specified connection properties and the "
        + "identifier of the document to retrieve. If the cluster has been configured for authorization and/or secure "
        + "transport (SSL/TLS), and the X-Pack plugin is available, secure connections can be made. This processor "
        + "supports Elasticsearch 5.x clusters.")
public class JsonQueryElasticsearch5 extends AbstractProcessor {

    private RestClient client;

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All original flowfiles that don't cause an error to occur go to this relationship").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be read from Elasticsearch are routed to this relationship").build();

    public static final Relationship REL_HITS = new Relationship.Builder().name("hits")
            .description("Search hits are routed to this relationship.")
            .build();

    public static final Relationship REL_AGGREGATIONS = new Relationship.Builder().name("aggregations")
            .description("Aggregations are routed to this relationship.")
            .build();

    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("el5-fetch-index")
            .displayName("Index")
            .description("The name of the index to read from")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("el5-type")
            .displayName("Type")
            .description("The type of this document (used by Elasticsearch for indexing and searching)")
            .defaultValue("")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("el5-query")
            .displayName("Query")
            .description("A query in JSON syntax, not Lucene syntax.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor HTTP_HOSTS = new PropertyDescriptor.Builder()
            .name("el5-http-hosts")
            .displayName("HTTP Hosts")
            .description("A comma-separated list of HTTP hosts that host ElasticSearch query nodes.")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("el5-ssl-context-service")
            .displayName("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections. This service only applies if the Elasticsearch endpoint(s) have been secured with TLS/SSL.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();
    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("el5-username")
            .displayName("Username")
            .description("The username to use with XPack security.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("el5-password")
            .displayName("Password")
            .description("The password to use with XPack security.")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("el5-connect-timeout")
            .displayName("Connect timeout")
            .description("Controls the amount of time, in milliseconds, before a timeout occurs when trying to connect.")
            .required(true)
            .defaultValue("5000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor SOCKET_TIMEOUT = new PropertyDescriptor.Builder()
            .name("el5-socket-timeout")
            .displayName("Read timeout")
            .description("Controls the amount of time, in milliseconds, before a timeout occurs when waiting for a response.")
            .required(true)
            .defaultValue("60000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor RETRY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("el5-retry-timeout")
            .displayName("Retry timeout")
            .description("Controls the amount of time, in milliseconds, before a timeout occurs when retrying the operation.")
            .required(true)
            .defaultValue("60000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final AllowableValue BREAK_UP_YES = new AllowableValue(
        "breakup-yes",
        "Yes",
        "Break up results."
    );
    public static final AllowableValue BREAK_UP_HITS_NO = new AllowableValue(
        "breakup-no",
        "No",
        "Don't break up results."
    );

    public static final PropertyDescriptor BREAK_UP_HITS = new PropertyDescriptor.Builder()
            .name("el5-break-up-hits")
            .displayName("Break up search results")
            .description("Break up search results into one flowfile per result.")
            .allowableValues(BREAK_UP_HITS_NO, BREAK_UP_YES)
            .defaultValue(BREAK_UP_HITS_NO.getValue())
            .required(true)
            .expressionLanguageSupported(false)
            .build();
    public static final PropertyDescriptor BREAK_UP_AGGREGATIONS = new PropertyDescriptor.Builder()
            .name("el5-break-up-aggregations")
            .displayName("Break up aggregation results")
            .description("Break up aggregation results into one flowfile per result.")
            .allowableValues(BREAK_UP_HITS_NO, BREAK_UP_YES)
            .defaultValue(BREAK_UP_HITS_NO.getValue())
            .required(true)
            .expressionLanguageSupported(false)
            .build();

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

    static {
        final Set<Relationship> _rels = new HashSet<>();
        _rels.add(REL_SUCCESS);
        _rels.add(REL_FAILURE);
        _rels.add(REL_HITS);
        _rels.add(REL_AGGREGATIONS);
        relationships = Collections.unmodifiableSet(_rels);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PROP_SSL_CONTEXT_SERVICE);
        descriptors.add(CONNECT_TIMEOUT);
        descriptors.add(SOCKET_TIMEOUT);
        descriptors.add(RETRY_TIMEOUT);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(HTTP_HOSTS);
        descriptors.add(QUERY);
        descriptors.add(INDEX);
        descriptors.add(TYPE);
        descriptors.add(BREAK_UP_HITS);
        descriptors.add(BREAK_UP_AGGREGATIONS);

        propertyDescriptors = Collections.unmodifiableList(descriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }


    @OnUnscheduled
    public void onStartup() {
        this.client = null;
    }

    private void setupClient(ProcessContext context) throws Exception {
        final String hosts = context.getProperty(HTTP_HOSTS).getValue();
        String[] hostsSplit = hosts.split(",[\\s]*");
        final SSLContextService sslService =
                context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final String username = context.getProperty(USERNAME).getValue();
        final String password = context.getProperty(PASSWORD).getValue();

        final Integer connectTimeout = context.getProperty(CONNECT_TIMEOUT).asInteger();
        final Integer readTimeout    = context.getProperty(SOCKET_TIMEOUT).asInteger();
        final Integer retryTimeout   = context.getProperty(RETRY_TIMEOUT).asInteger();

        HttpHost[] hh = new HttpHost[hostsSplit.length];
        for (int x = 0; x < hh.length; x++) {
            URL u = new URL(hostsSplit[x]);
            hh[x] = new HttpHost(u.getHost(), u.getPort(), u.getProtocol());
        }

        RestClientBuilder builder = RestClient.builder(hh)
        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                if (sslService != null) {
                    try {
                        KeyStore keyStore = KeyStore.getInstance(sslService.getKeyStoreType());
                        KeyStore trustStore = KeyStore.getInstance("JKS");

                        try (final InputStream is = new FileInputStream(sslService.getKeyStoreFile())) {
                            keyStore.load(is, sslService.getKeyStorePassword().toCharArray());
                        }

                        try (final InputStream is = new FileInputStream(sslService.getTrustStoreFile())) {
                            trustStore.load(is, sslService.getTrustStorePassword().toCharArray());
                        }

                        final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory
                                .getDefaultAlgorithm());
                        kmf.init(keyStore, sslService.getKeyStorePassword().toCharArray());
                        final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory
                                .getDefaultAlgorithm());
                        tmf.init(keyStore);
                        SSLContext context = SSLContext.getInstance(sslService.getSslAlgorithm());
                        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
                        httpClientBuilder = httpClientBuilder.setSSLContext(context);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                if (username != null && password != null) {
                    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(AuthScope.ANY,
                            new UsernamePasswordCredentials(username, password));
                    httpClientBuilder = httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }

                return httpClientBuilder;
            }
        })
        .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                return requestConfigBuilder.setConnectTimeout(connectTimeout)
                        .setSocketTimeout(readTimeout);
            }
        })
        .setMaxRetryTimeoutMillis(retryTimeout);

        this.client = builder.build();
    }

    private Response runQuery(String query, String index, String type) throws Exception {
        StringBuilder sb = new StringBuilder()
                .append("/")
                .append(index);
        if (type != null && !type.equals("")) {
            sb.append("/")
                .append(type);
        }

        sb.append("/_search");

        HttpEntity queryEntity = new NStringEntity(query, ContentType.APPLICATION_JSON);

        return client.performRequest("POST", sb.toString(), Collections.emptyMap(), queryEntity);
    }

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            if (this.client == null) {
                setupClient(context);
            }

            String query = context.getProperty(QUERY).evaluateAttributeExpressions(flowFile).getValue();
            String index = context.getProperty(INDEX).evaluateAttributeExpressions(flowFile).getValue();
            String type = context.getProperty(TYPE).evaluateAttributeExpressions(flowFile).getValue();

            Response resp = runQuery(query, index, type);

            InputStream content = resp.getEntity().getContent();
            String rawJson = IOUtils.toString(content, "UTF-8");

            Map<String, Object> response = mapper.readValue(rawJson, Map.class);
            Map<String, Object> hits     = (Map<String, Object>)response.get("hits");
            Map<String, Object> aggs     = (Map<String, Object>)response.get("aggregations");

            List<FlowFile> hitsFlowFiles = handleHits(hits, context, session, flowFile);
            List<FlowFile> aggsFlowFiles = handleAggregations(aggs, context, session, flowFile);

            if (hitsFlowFiles.size() > 0) {
                session.transfer(hitsFlowFiles, REL_HITS);
                for (FlowFile ff : hitsFlowFiles) {
                    session.getProvenanceReporter().create(ff);
                }
            }

            if (aggsFlowFiles.size() > 0) {
                session.transfer(aggsFlowFiles, REL_AGGREGATIONS);
                for (FlowFile ff : aggsFlowFiles) {
                    session.getProvenanceReporter().create(ff);
                }
            }
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception ex) {
            getLogger().error("Error processing flowfile.", ex);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private FlowFile writeAggregationFlowFileContents(String name, String json, ProcessSession session, FlowFile aggFlowFile) {
        aggFlowFile = session.write(aggFlowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(json.getBytes());
            }
        });
        if (name != null) {
            aggFlowFile = session.putAttribute(aggFlowFile, "aggregation.name", name);
        }

        return aggFlowFile;
    }

    private List<FlowFile> handleAggregations(Map<String, Object> aggregations, ProcessContext context, ProcessSession session, FlowFile parent) throws IOException {
        List<FlowFile> retVal = new ArrayList<>();
        if (aggregations == null) {
            return retVal;
        }
        String breakupValue = context.getProperty(BREAK_UP_AGGREGATIONS).getValue();

        if (breakupValue.equals(BREAK_UP_YES.getValue())) {
            for (Map.Entry<String, Object> agg : aggregations.entrySet()) {
                FlowFile aggFlowFile = session.create(parent);
                String aggJson = mapper.writeValueAsString(agg.getValue());
                retVal.add(writeAggregationFlowFileContents(agg.getKey(), aggJson, session, aggFlowFile));
            }
        } else {
            String json = mapper.writeValueAsString(aggregations);
            retVal.add(writeAggregationFlowFileContents(null, json, session, session.create(parent)));
        }

        return retVal;
    }

    private FlowFile writeHitFlowFile(String json, ProcessSession session, FlowFile hitFlowFile) {
        return session.write(hitFlowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(json.getBytes());
            }
        });
    }

    private List<FlowFile> handleHits(Map<String, Object> hits, ProcessContext context, ProcessSession session, FlowFile parent) throws IOException {
        List<Map<String, Object>> hitsBranch = (List<Map<String, Object>>)hits.get("hits");
        String breakupValue = context.getProperty(BREAK_UP_HITS).getValue();
        List<FlowFile> retVal = new ArrayList<>();
        if (breakupValue.equals(BREAK_UP_YES.getValue())) {
            for (Map<String, Object> hit : hitsBranch) {
                FlowFile hitFlowFile = session.create(parent);
                String json = mapper.writeValueAsString(hit);

                retVal.add(writeHitFlowFile(json, session, hitFlowFile));
            }
        } else {
            FlowFile hitFlowFile = session.create(parent);
            String json = mapper.writeValueAsString(hitsBranch);
            retVal.add(writeHitFlowFile(json, session, hitFlowFile));
        }

        return retVal;
    }
}
