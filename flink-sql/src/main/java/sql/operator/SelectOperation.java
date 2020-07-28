package sql.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import sql.source.KafkaSource;

public class SelectOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //以后版本会将old planner移除
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        KafkaSource.getKafkaSource(tableEnv);
        Table table1 = tableEnv.sqlQuery("select user_id,item_id,category_id,behavior,ts," +
                "proctime from user_behavior where behavior='buy'");
//        tableEnv.toAppendStream(table1, Behavior.class).print();
        tableEnv.toAppendStream(table1, Row.class).print();
        env.execute();

        /**
         * 用POJO时报错
         * 当前1.10.0版本的bug，1.10.1已经修复--已确认修复
         * Exception in thread "main" org.apache.flink.table.api.ValidationException: Field types of query result and
         * registered TableSink  do not match.
         * Query schema: [user_id: BIGINT, item_id: BIGINT, category_id: BIGINT, behavior: STRING, ts: TIMESTAMP(3)
         * *ROWTIME*, proctime: TIMESTAMP(3) NOT NULL *PROCTIME*]
         * Sink schema: [behavior: STRING, category_id: BIGINT, item_id: BIGINT, proctime: TIMESTAMP(3), ts:
         * TIMESTAMP(3), user_id: BIGINT]
         * 	at org.apache.flink.table.planner.sinks.TableSinkUtils$.validateSchemaAndApplyImplicitCast(TableSinkUtils
         * 	.scala:96)
         * 	at org.apache.flink.table.planner.delegation.PlannerBase.translateToRel(PlannerBase.scala:229)
         * 	at org.apache.flink.table.planner.delegation.PlannerBase$$anonfun$1.apply(PlannerBase.scala:150)
         * 	at org.apache.flink.table.planner.delegation.PlannerBase$$anonfun$1.apply(PlannerBase.scala:150)
         * 	at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
         * 	at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
         * 	at scala.collection.Iterator$class.foreach(Iterator.scala:891)
         * 	at scala.collection.AbstractIterator.foreach(Iterator.scala:1334)
         * 	at scala.collection.IterableLike$class.foreach(IterableLike.scala:72)
         * 	at scala.collection.AbstractIterable.foreach(Iterable.scala:54)
         * 	at scala.collection.TraversableLike$class.map(TraversableLike.scala:234)
         * 	at scala.collection.AbstractTraversable.map(Traversable.scala:104)
         * 	at org.apache.flink.table.planner.delegation.PlannerBase.translate(PlannerBase.scala:150)
         * 	at org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl.toDataStream
         * 	(StreamTableEnvironmentImpl.java:351)
         * 	at org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl.toAppendStream
         * 	(StreamTableEnvironmentImpl.java:259)
         * 	at org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl.toAppendStream
         * 	(StreamTableEnvironmentImpl.java:250)
         * 	at sql.KafkaSourceTable.main(KafkaSourceTable.java:35)
         */

    }


}
