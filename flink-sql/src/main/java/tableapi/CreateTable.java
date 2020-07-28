package tableapi;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Random;

public class CreateTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //以后版本会将old planner移除,1.11默认planner已变为blink planner
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        DataStreamSource<Tuple2<String, Integer>> src =
                env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                Random random = new Random();
                while (true) {
                    ctx.collect(Tuple2.of("field1-" + random.nextInt(1000), random.nextInt(1000)));
                    Thread.sleep(1000L);
                }
            }

            @Override
            public void cancel() {

            }
        });
        //从datastream生成动态表
        /**
         * 方法一：
         * Table table = tableEnv.fromDataStream(src);
         * 过滤出大于500的值
         * Table table1 = table.select("*").where("f1>500");
         */

        //方法二：注册为临时表
        tableEnv.createTemporaryView("mytable",src);
        //过滤出大于500的值
        Table table1 = tableEnv.sqlQuery("select * from mytable where f1>500");
        //table转成datastream
        //https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/common.html#convert-a-table-into-a-datastream-or-dataset
        tableEnv.toAppendStream(table1, TypeInformation.of(new TypeHint<Tuple2<String,Integer>>() {
            @Override
            public TypeInformation<Tuple2<String,Integer>> getTypeInfo() {
                return super.getTypeInfo();
            }
        })).print();
        env.execute();

    }
}
