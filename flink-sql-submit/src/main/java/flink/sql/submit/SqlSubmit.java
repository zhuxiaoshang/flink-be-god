package flink.sql.submit;

import flink.sql.submit.cli.CliOptions;
import flink.sql.submit.cli.CliOptionsParser;
import flink.sql.submit.cli.SqlCommandParser;
import flink.sql.submit.cli.SqlCommandParser.SqlCommandCall;
import flink.sql.submit.utils.OSSUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.util.FlinkRuntimeException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static flink.sql.submit.cli.SqlCommandParser.SqlCommand.SET;

/**
 * @author: zhushang
 * @create: 2020-11-05 17:14
 */
public class SqlSubmit {

    public static void main(String[] args) throws Exception {
        final CliOptions options = CliOptionsParser.parseClient(args);
        SqlSubmit submit = new SqlSubmit(options);
        submit.run();
    }

    // --------------------------------------------------------------------------------------------

    private String sqlFilePath;
    private String workSpace;
    private String jobName;
    private String[] udfUrls;
    private TableEnvironment tEnv;

    private SqlSubmit(CliOptions options) {
        this.sqlFilePath = options.getSqlFilePath();
        this.workSpace = options.getWorkingSpace();
        this.udfUrls = options.getUdfUrls();
        this.jobName = options.getJobName();
    }

    private void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        this.tEnv = StreamTableEnvironment.create(env, settings);

        String name = "hive_catalog";
        String defaultDatabase = "defaultdb";
        String hiveConfDir = "/opt/hive-conf"; // a local path

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tEnv.registerCatalog("hive_catalog", hive);
        tEnv.useCatalog("hive_catalog");

        URLClassLoader classLoader = (URLClassLoader) tEnv.getClass().getClassLoader();
        if (udfUrls != null && udfUrls.length > 0) {
            List<String> urls = OSSUtils.downloadFile(jobName, udfUrls);
            loadUdfJar(classLoader, getUrls(urls));
            addJarToPath(env, urls);
        }

        List<String> sql = Files.readAllLines(Paths.get(workSpace + "/" + sqlFilePath));
        List<SqlCommandCall> calls = SqlCommandParser.parse(sql);
        StatementSet statementSet = tEnv.createStatementSet();
        for (SqlCommandCall call : calls) {
            callCommand(call, statementSet);
        }
        statementSet.execute();
    }

    private void addJarToPath(StreamExecutionEnvironment env, List<String> urls) throws Exception {
        Field configuration = StreamExecutionEnvironment.class.getDeclaredField("configuration");
        configuration.setAccessible(true);
        Configuration o = (Configuration) configuration.get(env);

        Field confData = Configuration.class.getDeclaredField("confData");
        confData.setAccessible(true);
        Map<String, Object> temp = (Map<String, Object>) confData.get(o);
        //        temp.put("pipeline.classpaths", Arrays.asList(udfUrls));
        temp.put("pipeline.jars", urls);
    }

    private List<URL> getUrls(List<String> urls) {
        return urls.stream()
                .map(
                        url -> {
                            try {
                                return new URL(url);
                            } catch (MalformedURLException e) {
                                e.printStackTrace();
                            }
                            return null;
                        })
                .filter(url -> url != null)
                .collect(Collectors.toList());
    }

    private void loadUdfJar(URLClassLoader classLoader, List<URL> jarUrlList) {
        // 从URLClassLoader类加载器中获取类的addURL方法
        Method method = null;

        try {
            method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        } catch (NoSuchMethodException | SecurityException e1) {
            e1.printStackTrace();
        }

        try {
            method.setAccessible(true);
            // jar路径加入到系统url路径里
            URLClassLoader systemClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
            for (URL jarUrl : jarUrlList) {
                method.invoke(systemClassLoader, jarUrl);
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException("load udf [" + jarUrlList + "] failed!", e);
        }
    }

    // --------------------------------------------------------------------------------------------

    private void callCommand(SqlCommandCall cmdCall, StatementSet statementSet) {
        if (cmdCall.command == SET) {
            callSet(cmdCall);
        } else {
            executeSql(cmdCall, statementSet);
        }
    }

    private void callSet(SqlCommandCall cmdCall) {
        String key = cmdCall.operands[0];
        String value = cmdCall.operands[1];
        tEnv.getConfig().getConfiguration().setString(key, value);
    }

    private void executeSql(SqlCommandCall cmdCall, StatementSet statementSet) {
        String sql = cmdCall.operands[0];
        try {
            if (sql.startsWith("insert into") || sql.startsWith("INSERT INTO")) {
                statementSet.addInsertSql(sql);
            } else {
                tEnv.executeSql(sql);
            }
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + sql + "\n", e);
        }
    }
}
