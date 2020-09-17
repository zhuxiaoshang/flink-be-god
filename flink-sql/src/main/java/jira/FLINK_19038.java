package jira;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * https://issues.apache.org/jira/browse/FLINK-19038
 */
public class FLINK_19038 {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tbEnv = TableEnvironment.create(settings);
        Table t1 = tbEnv.fromValues("1", "2", "3", "4", "5", "6", "7", "8", "9");
        Table t2 = t1.orderBy("0").fetch(5).fetch(6);
        tbEnv.executeSql("create table print(c1 String) with ('connector' = 'print')");
        tbEnv.insertInto("print",t2);
        tbEnv.execute("");
    }
}
