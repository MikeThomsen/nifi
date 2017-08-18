package org.apache.nifi.processors.elasticsearch;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

@Ignore("These are integration tests and require ElasticSearch 5.X. If you use a Docker image, you'll have to configure it to expose the host's IP address in the cluster state.")
public class TestJsonQueryElasticsearch5 {
    private static final List<String> msgs = Arrays.asList(
            "one", "two", "three", "four", "five"
    );

    private static TransportClient client;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String INDEX_NAME;

    static {
        INDEX_NAME = "messages_" + Calendar.getInstance().getTimeInMillis();
    }

    @BeforeClass
    public static void beforeClass() throws InterruptedException {
        client = new PreBuiltTransportClient(Settings.EMPTY);
        client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("localhost", 9300)));
        CreateIndexRequestBuilder builder = client.admin().indices().prepareCreate(INDEX_NAME);
        String source = "{\n" +
                "\t\"properties\": {\n" +
                "\t\t\"msg\": {\n" +
                "\t\t\t\"type\": \"keyword\"\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";
        builder.addMapping("message", source);
        CreateIndexResponse response = builder.get();
        int[] counts = new int[msgs.size()];
        Random random = new Random();
        for (int x = 0; x < counts.length; x++) {
            counts[x] = random.nextInt(100);
            for (int y  = 0; y < counts.length; y++) {
                String body = String.format("{ \"msg\": \"%s\" }", msgs.get(y));
                client.prepareIndex(INDEX_NAME, "msg").setSource(body).get();
            }
        }

        Thread.sleep(5000);
    }

    @AfterClass
    public static void afterClass() {
        client.admin().indices().prepareDelete(INDEX_NAME).get();
        client.close();
    }

    public void testCounts(TestRunner runner, int success, int hits, int failure, int aggregations) {
        runner.assertTransferCount(JsonQueryElasticsearch5.REL_SUCCESS, success);
        runner.assertTransferCount(JsonQueryElasticsearch5.REL_HITS, hits);
        runner.assertTransferCount(JsonQueryElasticsearch5.REL_FAILURE, failure);
        runner.assertTransferCount(JsonQueryElasticsearch5.REL_AGGREGATIONS, aggregations);
    }

    @Test
    public void testBasicQuery() throws Exception {

        JsonQueryElasticsearch5 processor = new JsonQueryElasticsearch5();
        TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(JsonQueryElasticsearch5.HTTP_HOSTS, "http://localhost:9200");
        runner.setProperty(JsonQueryElasticsearch5.INDEX, INDEX_NAME);
        runner.setProperty(JsonQueryElasticsearch5.TYPE, "msg");
        runner.setValidateExpressionUsage(true);
        runner.setProperty(JsonQueryElasticsearch5.QUERY, "{ \"query\": { \"match_all\": {} }}");

        runner.enqueue("test");
        runner.run(1, true, true);
        testCounts(runner, 1, 1, 0, 0);

        runner.setProperty(JsonQueryElasticsearch5.BREAK_UP_HITS, JsonQueryElasticsearch5.BREAK_UP_YES);
        runner.clearProvenanceEvents();
        runner.clearTransferState();
        runner.enqueue("test");
        runner.run(1, true, true);
        testCounts(runner, 1, 10, 0, 0);
    }

    @Test
    public void testAggregations() throws Exception {
        String query = "{\n" +
                "\t\"query\": {\n" +
                "\t\t\"match_all\": {}\n" +
                "\t},\n" +
                "\t\"aggs\": {\n" +
                "\t\t\"test_agg\": {\n" +
                "\t\t\t\"terms\": {\n" +
                "\t\t\t\t\"field\": \"msg\"\n" +
                "\t\t\t}\n" +
                "\t\t},\n" +
                "\t\t\"test_agg2\": {\n" +
                "\t\t\t\"terms\": {\n" +
                "\t\t\t\t\"field\": \"msg\"\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";


        JsonQueryElasticsearch5 processor = new JsonQueryElasticsearch5();
        TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(JsonQueryElasticsearch5.HTTP_HOSTS, "http://localhost:9200");
        runner.setProperty(JsonQueryElasticsearch5.INDEX, INDEX_NAME);
        runner.setProperty(JsonQueryElasticsearch5.TYPE, "msg");
        runner.setValidateExpressionUsage(true);
        runner.setProperty(JsonQueryElasticsearch5.QUERY, query);

        runner.enqueue("test");
        runner.run(1, true, true);
        testCounts(runner, 1, 1, 0, 1);

        runner.clearTransferState();
        runner.clearProvenanceEvents();
        runner.setProperty(JsonQueryElasticsearch5.BREAK_UP_AGGREGATIONS, JsonQueryElasticsearch5.BREAK_UP_YES);
        runner.enqueue("test");
        runner.run(1, true, true);
        testCounts(runner, 1, 1, 0, 2);

        runner.clearProvenanceEvents();
        runner.clearTransferState();

        query = "{\n" +
                "\t\"query\": {\n" +
                "\t\t\"match_all\": {}\n" +
                "\t},\n" +
                "\t\"aggs\": {\n" +
                "\t\t\"test_agg\": {\n" +
                "\t\t\t\"terms\": {\n" +
                "\t\t\t\t\"field\": \"${fieldValue}\"\n" +
                "\t\t\t}\n" +
                "\t\t},\n" +
                "\t\t\"test_agg2\": {\n" +
                "\t\t\t\"terms\": {\n" +
                "\t\t\t\t\"field\": \"${fieldValue}\"\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";
        runner.setVariable("fieldValue", "msg");
        runner.setVariable("es.index", INDEX_NAME);
        runner.setVariable("es.type", "msg");
        runner.setProperty(JsonQueryElasticsearch5.QUERY, query);
        runner.setProperty(JsonQueryElasticsearch5.INDEX, "${es.index}");
        runner.setProperty(JsonQueryElasticsearch5.TYPE, "${es.type}");
        runner.setValidateExpressionUsage(true);
        runner.enqueue("test");
        runner.run(1, true, true);
        testCounts(runner, 1, 1, 0, 2);
    }
}
