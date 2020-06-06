package connector.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import java.util.Properties;

/**
 * kafka connector,支持source/sink
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/kafka.html
 */
public class KafkaConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<String> src = getKafkaSource(env);
        src.addSink(getKafkaSink());
        env.execute();
    }

    public static DataStream<String> getKafkaSource(StreamExecutionEnvironment env) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
//        properties.setProperty("group.id", "groupid");
        //开始消费的offset位置，支持latest, earliest, none
        properties.setProperty("auto.offset.reset", "earliest");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("user_behavior", new SimpleStringSchema(), properties));
        return stream;
    }

    public static FlinkKafkaProducer getKafkaSink() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "broker");
        return new FlinkKafkaProducer( "target_topic", (KafkaSerializationSchema) new SimpleStringSchema(),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}
