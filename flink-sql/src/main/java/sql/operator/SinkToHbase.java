package sql.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import sql.sink.HbaseSink;
import sql.source.KafkaSource;

/**
 * 从kafka读数据写入hbase中
 * 这里涉及到搭建hadoop、hbase、zk环境的过程，hbase不使用内置的zk，因为kafka也需要用zk,所以让他们公用一个zk，涉及到很多配置比较麻烦
 * 比较重要的几个配置如下：
 * 1.hadoop的etc/hadoop/core-site.xml
 *   <property>
 *     <name>fs.defaultFS</name>
 *     <value>hdfs://localhost:9000</value>
 *   </property>
 * 2. hbase的conf/hbase-env.sh
 * export HBASE_MANAGES_ZK=false   false表示使用外部zk
 * 3. hbase的conf/hbase-site.xml
 * <property>
 *     <name>hbase.rootdir</name>
 *     //这里设置让HBase存储文件的地方,要和上面hadoop的fs.defaultFS一致
 *     <value>hdfs://localhost:9000/user/hbase</value>
 *   </property>
 *    <property>
 *         <name>hbase.cluster.distributed</name>
 *         <value>true</value>//同样表示使用外部zk
 *     </property>
 *
 *  <p>详细配置我会上传到仓库</p>
 */
public class SinkToHbase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        KafkaSource.getKafkaSource(tableEnv);//create kafka source table user_behavior
        HbaseSink.getHbaseSink(tableEnv);
        Table table1 = tableEnv.sqlQuery("SELECT concat(cast(user_id as String),cast(item_id as String)) as row_key,ROW(user_id, item_id,category_id," +
                "behavior) as col_family" +
                "\n" +
                "FROM user_behavior\n" +
                "WHERE behavior = 'buy'");
        table1.executeInsert("hbaseSinkTable");
//        env.execute();
    }
    /**
     * 结果
     * hbase(main):007:0> scan 'hbase_test',{LIMIT=>10}
     * ROW                                                  COLUMN+CELL
     *  10000033557403                                      column=cf:behavior, timestamp=1590860596937, value=buy
     *  10000033557403                                      column=cf:category_id, timestamp=1590860596937,
     *  value=\x00\x00\x00\x00\x00\x06F\x11
     *  10000033557403                                      column=cf:item_id, timestamp=1590860596937,
     *  value=\x00\x00\x00\x00\x006H\x1B
     *  10000033557403                                      column=cf:user_id, timestamp=1590860596937,
     *  value=\x00\x00\x00\x00\x00\x0FBC
     *  10000173722681                                      column=cf:behavior, timestamp=1590860595873, value=buy
     *  10000173722681                                      column=cf:category_id, timestamp=1590860595873,
     *  value=\x00\x00\x00\x00\x00\x1BFv
     *  10000173722681                                      column=cf:item_id, timestamp=1590860595873,
     *  value=\x00\x00\x00\x00\x008\xCD\xB9
     *  10000173722681                                      column=cf:user_id, timestamp=1590860595873,
     *  value=\x00\x00\x00\x00\x00\x0FBQ
     *  10000224910145                                      column=cf:behavior, timestamp=1590860597805, value=buy
     *  10000224910145                                      column=cf:category_id, timestamp=1590860597805,
     *  value=\x00\x00\x00\x00\x00 \x19\xD8
     *  10000224910145                                      column=cf:item_id, timestamp=1590860597805,
     *  value=\x00\x00\x00\x00\x00J\xECA
     *  10000224910145                                      column=cf:user_id, timestamp=1590860597805,
     *  value=\x00\x00\x00\x00\x00\x0FBV
     *  10000514560771                                      column=cf:behavior, timestamp=1590860603925, value=buy
     *  10000514560771                                      column=cf:category_id, timestamp=1590860603925,
     *  value=\x00\x00\x00\x00\x00\x15\x00f
     *  10000514560771                                      column=cf:item_id, timestamp=1590860603925,
     *  value=\x00\x00\x00\x00\x00E\x97\x83
     *  10000514560771                                      column=cf:user_id, timestamp=1590860603925,
     *  value=\x00\x00\x00\x00\x00\x0FBs
     *  10000543916598                                      column=cf:behavior, timestamp=1590860607024, value=buy
     *  10000543916598                                      column=cf:category_id, timestamp=1590860607024,
     *  value=\x00\x00\x00\x00\x00F;\xD9
     *  10000543916598                                      column=cf:item_id, timestamp=1590860607024,
     *  value=\x00\x00\x00\x00\x00;\xC36
     *  10000543916598                                      column=cf:user_id, timestamp=1590860607024,
     *  value=\x00\x00\x00\x00\x00\x0FBv
     *  100007894989                                        column=cf:behavior, timestamp=1590860608109, value=buy
     *  100007894989                                        column=cf:category_id, timestamp=1590860608109,
     *  value=\x00\x00\x00\x00\x00\x1E\xCE\x15
     *  100007894989                                        column=cf:item_id, timestamp=1590860608109,
     *  value=\x00\x00\x00\x00\x00\x01s\x0D
     *  100007894989                                        column=cf:user_id, timestamp=1590860608109,
     *  value=\x00\x00\x00\x00\x00\x0FB\x8E
     *  10000842086401                                      column=cf:behavior, timestamp=1590860604390, value=buy
     *  10000842086401                                      column=cf:category_id, timestamp=1590860604390,
     *  value=\x00\x00\x00\x00\x00\x00\xFFR
     *  10000842086401                                      column=cf:item_id, timestamp=1590860604390,
     *  value=\x00\x00\x00\x00\x00\x1F\xD6\x01
     *  10000842086401                                      column=cf:user_id, timestamp=1590860604390,
     *  value=\x00\x00\x00\x00\x00\x0FB\x94
     *  10000842675133                                      column=cf:behavior, timestamp=1590860604390, value=buy
     *  10000842675133                                      column=cf:category_id, timestamp=1590860604390,
     *  value=\x00\x00\x00\x00\x00\x00\xFFR
     *  10000842675133                                      column=cf:item_id, timestamp=1590860604390,
     *  value=\x00\x00\x00\x00\x00(\xD1\xBD
     *  10000842675133                                      column=cf:user_id, timestamp=1590860604390,
     *  value=\x00\x00\x00\x00\x00\x0FB\x94
     *  10000843350364                                      column=cf:behavior, timestamp=1590860606832, value=buy
     *  10000843350364                                      column=cf:category_id, timestamp=1590860606832,
     *  value=\x00\x00\x00\x00\x00;|s
     *  10000843350364                                      column=cf:item_id, timestamp=1590860606832,
     *  value=\x00\x00\x00\x00\x003\x1F\x5C
     *  10000843350364                                      column=cf:user_id, timestamp=1590860606832,
     *  value=\x00\x00\x00\x00\x00\x0FB\x94
     *  10000881628033                                      column=cf:behavior, timestamp=1590860612671, value=buy
     *  10000881628033                                      column=cf:category_id, timestamp=1590860612671,
     *  value=\x00\x00\x00\x00\x00,\x08\x0A
     *  10000881628033                                      column=cf:item_id, timestamp=1590860612671,
     *  value=\x00\x00\x00\x00\x00\x18\xD7\x81
     *  10000881628033                                      column=cf:user_id, timestamp=1590860612671,
     *  value=\x00\x00\x00\x00\x00\x0FB\x98
     * 10 row(s)
     * Took 0.1680 seconds
     */

}
