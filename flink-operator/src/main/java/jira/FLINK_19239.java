package jira;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

/**
 * 这个问题同FLINK-19133,https://issues.apache.org/jira/browse/FLINK-19133
 * 问题出现场景：1.streaming 任务
 *            2.sql任务，如果指定了sink.partitioner配置
 * 问题原因：针对streaming任务，在new FlinkKafkaProducer时，如果不指定partitioner，则会使用FlinkFixedPartitioner来做分区，最终partitioner会传递给KafkaSerializationSchemaWrapper，在produce数据时调用
 * FlinkFixedPartitioner#partition来进行分区，然而parallelInstanceId一直没有赋值，所以总是0，导致数据最终都会发向topic的0partition
 * sql任务 如果指定了sink.partitioner则和上面一样
 *        如果没有指定partitioner，那么递给KafkaSerializationSchemaWrapper的就为null，在调用KafkaProducer进行send时会根据record中的partition进行分区，此时为null则使用kafka自身的
 *        DefaultPartitioner进行分区，如果指定了key则按key进行hash，如果没指定则随机发送
 *
 * @author: zhushang
 * @create: 2020-09-17 15:14
 **/

public class FLINK_19239 {
    public static void main1(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");//user_behavior
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("user_behavior", new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        DataStream<String> stream = env
                .addSource(consumer);
        stream.print();
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>("localhost:9092", "test6",new SimpleStringSchema());
        stream.addSink(producer);
        env.execute();
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(env, settings);
        tbEnv.executeSql("CREATE TABLE kafka_source (\n" +
                "    user_id BIGINT,\n" +
                "    item_id BIGINT,\n" +
                "    category_id BIGINT,\n" +
                "    behavior STRING,\n" +
                "    ts TIMESTAMP(3),\n" +
                "    proctime as PROCTIME(),   -- 通过计算列产生一个处理时间列\n" +
                "    WATERMARK FOR ts as ts - INTERVAL '5' SECOND  -- 在ts上定义watermark，ts成为事件时间列\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',  -- 使用 kafka connector\n" +
                "    'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本\n" +
                "    'connector.topic' = 'user_behavior',  -- kafka topic\n" +
                "    'connector.startup-mode' = 'earliest-offset',  -- 从起始 offset 开始读取\n" +
                "    'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zookeeper 地址\n" +
                "    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker 地址\n" +
                "    'format.type' = 'json'  -- 数据源格式为 json\n" +
                ")");
        tbEnv.executeSql("CREATE TABLE kafka_sink (\n" +
                "    user_id BIGINT,\n" +
                "    item_id BIGINT,\n" +
                "    category_id BIGINT,\n" +
                "    behavior STRING,\n" +
                "    ts TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',  -- 使用 kafka connector\n" +
                "    'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本\n" +
                "    'connector.topic' = 'test6',  -- kafka topic\n" +
                "    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker 地址\n" +
                "    'format.type' = 'json',  -- 数据源格式为 json\n" +
                "    'sink.partitioner' = 'fixed' --如果指定的话同样会遇到该问题"+
                ")");
        tbEnv.executeSql("insert into kafka_sink select user_id,item_id,category_id,behavior,ts from kafka_source");

    }
}
