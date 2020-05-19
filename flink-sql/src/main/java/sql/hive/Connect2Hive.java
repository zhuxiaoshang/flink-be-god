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
        String hiveConfDir = "C:\\Users\\17020047\\Desktop\\工具\\hive-site.xml";
        String version = "1.2.2";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hive);
        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");
        for(String catalog:tableEnv.listDatabases()){
            System.out.println(catalog);
        }
        tableEnv.useDatabase("bi_dm");
        Table table = tableEnv.sqlQuery("select * from dm_area_str_td");
        table.toString();
        tableEnv.execute("");
    }
}
