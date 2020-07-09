package sql.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Connect2Hive {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name = "myhive";
        String defaultDatabase = "mydatabase";
        // a local path
        String hiveConfDir = "/Users/zhushang/Desktop/software/apache-hive-2.2.0-bin/conf";
        String version = "2.2.0 ";

//        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
//        tableEnv.registerCatalog("myhive", hive);
        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");
        for(String catalog:tableEnv.listDatabases()){
            System.out.println(catalog);
        }
        tableEnv.useDatabase("default");
        Table table = tableEnv.sqlQuery("select * from dm_area_str_td");
        table.toString();
        tableEnv.execute("");
    }
}
