package operator.asyncio;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 此类采用第二种方案--线程池模拟异步客户端
 */
public class ASyncIOFunction extends RichAsyncFunction<CategoryInfo, CategoryInfo> {
    private transient MysqlSyncClient client;
    private ExecutorService executorService;

    @Override
    public void asyncInvoke(CategoryInfo input, ResultFuture<CategoryInfo> resultFuture) throws Exception {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                resultFuture.complete(Collections.singletonList((CategoryInfo) client.query(input)));
            }
        });

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        client = new MysqlSyncClient();
        executorService = new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
    }

    @Override
    public void close() throws Exception {
        super.close();
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
