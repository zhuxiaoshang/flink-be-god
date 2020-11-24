package flink.sql.submit;

import flink.sql.submit.cli.CliOptions;
import flink.sql.submit.cli.CliOptionsParser;
import flink.sql.submit.cli.SqlCommandParser;
import flink.sql.submit.cli.SqlCommandParser.SqlCommandCall;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static flink.sql.submit.cli.SqlCommandParser.SqlCommand.SET;

/**
 * @author: zhushang
 * @create: 2020-11-05 17:14
 **/
public class SqlSubmit {

    public static void main(String[] args) throws Exception {
        final CliOptions options = CliOptionsParser.parseClient(args);
        SqlSubmit submit = new SqlSubmit(options);
        submit.run();
    }

    // --------------------------------------------------------------------------------------------

    private String sqlFilePath;
    private String workSpace;
    private TableEnvironment tEnv;

    private SqlSubmit(CliOptions options) {
        this.sqlFilePath = options.getSqlFilePath();
        this.workSpace = options.getWorkingSpace();
    }

    private void run() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        this.tEnv = TableEnvironment.create(settings);

        String name            = "hive_catalog";
        String defaultDatabase = "defaultdb";
        String hiveConfDir     = "/opt/hive-conf"; // a local path

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tEnv.registerCatalog("hive_catalog", hive);
        tEnv.useCatalog("hive_catalog");

        List<String> sql = Files.readAllLines(Paths.get(workSpace + "/" + sqlFilePath));
        List<SqlCommandCall> calls = SqlCommandParser.parse(sql);
        for (SqlCommandCall call : calls) {
            callCommand(call);
        }
//        tEnv.execute("SQL Job");
    }

    // --------------------------------------------------------------------------------------------

    private void callCommand(SqlCommandCall cmdCall) {
        if(cmdCall.command == SET){
            callSet(cmdCall);
        }else{
            executeSql(cmdCall);
        }
    }

    private void callSet(SqlCommandCall cmdCall) {
        String key = cmdCall.operands[0];
        String value = cmdCall.operands[1];
        tEnv.getConfig().getConfiguration().setString(key, value);
    }

    private void executeSql(SqlCommandCall cmdCall) {
        String ddl = cmdCall.operands[0];
        try {
            tEnv.executeSql(ddl);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
    }
}
