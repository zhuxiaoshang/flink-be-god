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
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.List;

/**
 * 使用mysql异步客户端方式
 */
public class ASyncIOClientFunction extends RichAsyncFunction<CategoryInfo, CategoryInfo> {
    private transient SQLClient mySQLClient;
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://localhost:3306/flink";
    private static final String USER = "root";
    private static final String PASSWORD = "123456";

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
    public void asyncInvoke(CategoryInfo input, ResultFuture<CategoryInfo> resultFuture) throws Exception {

        mySQLClient.getConnection(new Handler<AsyncResult<SQLConnection>>() {
            @Override
            public void handle(AsyncResult<SQLConnection> sqlConnectionAsyncResult) {
                if (sqlConnectionAsyncResult.failed()) {
                    return;
                }
                SQLConnection connection = sqlConnectionAsyncResult.result();
                connection.query("select sub_category_id,parent_category_id from category where sub_category_id " +
                        "='" + input.getSubCategoryId() + "';", new Handler<AsyncResult<ResultSet>>() {
                    @Override
                    public void handle(AsyncResult<ResultSet> resultSetAsyncResult) {
                        if (resultSetAsyncResult.succeeded()) {
                            List<JsonObject> rows = resultSetAsyncResult.result().getRows();
                            for (JsonObject jo :
                                    rows) {
                                resultFuture.complete(Collections.singletonList(new CategoryInfo(jo.getLong("sub_category_id"),
                                        jo.getLong("parent_category_id")
                                )));

                            }
                        }
                    }
                });
            }
        });
    }

    /**
     * {@link AsyncFunction#asyncInvoke} timeout occurred.
     * By default, the result future is exceptionally completed with a timeout exception.
     *
     * @param input        element coming from an upstream task
     * @param resultFuture to be completed with the result data
     */
    @Override
    public void timeout(CategoryInfo input, ResultFuture<CategoryInfo> resultFuture) throws Exception {
        System.out.println("async call time out!");
        input.setParentCategoryId(Long.MIN_VALUE);
        resultFuture.complete(Collections.singleton(input));
    }
}
