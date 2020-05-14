package connector.es;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * flink 往ES中写数据
 */
public class ESConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> input = env.addSource(new ParallelSourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int i =1;
                while (true) {
                    ctx.collect(new JSONObject().fluentPut("key"+ i++,"value").toJSONString());
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        ElasticsearchSink.Builder<String> esSinkBuilder = getESSink();
        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they
        // would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);
        // finally, build and add the sink to the job's pipeline
        input.addSink(esSinkBuilder.build());
        env.execute();
    }

    public static ElasticsearchSink.Builder<String> getESSink() {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200, "http"));
        httpHosts.add(new HttpHost("10.2.3.1", 9200, "http"));


// use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("data", element);

                        return Requests.indexRequest()
                                .index("my-index")
                                .type("my-type")
                                .source(json);
                    }
                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        return esSinkBuilder;
    }
}
