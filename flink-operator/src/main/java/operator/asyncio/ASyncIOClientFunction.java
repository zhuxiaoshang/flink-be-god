package operator.asyncio;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.List;

/**
 * 使用mysql异步客户端方式
 */
public class ASyncIOClientFunction extends RichAsyncFunction<StoreInfo, StoreInfo> {
    private transient SQLClient mySQLClient;
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://xxxx";
    private static final String USER = "xxxx";
    private static final String PASSWORD = "xxxx";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JsonObject mySQLClientConfig = new JsonObject();
        mySQLClientConfig.put("url", URL)
                .put("driver_class", JDBC_DRIVER)
                .put("max_pool_size", 20)
                .put("user", USER)
                .put("password", PASSWORD);

        VertxOptions vo = new VertxOptions();
        vo.setEventLoopPoolSize(10);
        vo.setWorkerPoolSize(20);
        Vertx vertx = Vertx.vertx(vo);
        mySQLClient = JDBCClient.createNonShared(vertx, mySQLClientConfig);
    }

    @Override
    public void close() throws Exception {
        super.close();
        mySQLClient.close();
    }

    @Override
    public void asyncInvoke(StoreInfo input, ResultFuture<StoreInfo> resultFuture) throws Exception {

        mySQLClient.getConnection(new Handler<AsyncResult<SQLConnection>>() {
            @Override
            public void handle(AsyncResult<SQLConnection> sqlConnectionAsyncResult) {
                if (sqlConnectionAsyncResult.failed()) {
                    return;
                }
                SQLConnection connection = sqlConnectionAsyncResult.result();
                connection.query("select str_cd,crf_str_cd,crf_str_nm from t_shp_crf_str_cfg_ed_a where str_cd " +
                        "='" + input.getStrCd() + "';", new Handler<AsyncResult<ResultSet>>() {
                    @Override
                    public void handle(AsyncResult<ResultSet> resultSetAsyncResult) {
                        if (resultSetAsyncResult.succeeded()) {
                            List<JsonObject> rows = resultSetAsyncResult.result().getRows();
                            for (JsonObject jo :
                                    rows) {
                                resultFuture.complete(Collections.singletonList(new StoreInfo(jo.getString("str_cd"),
                                        jo.getString("crf_str_nm")
                                )));

                            }
                        }
                    }
                });
            }
        });
    }
}
