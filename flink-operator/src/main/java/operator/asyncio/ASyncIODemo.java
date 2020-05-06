package operator.asyncio;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * 使用异步io的先决条件
 * 1.数据库(或key/value存储)提供支持异步请求的client。
 * 2.没有异步请求客户端的话也可以将同步客户端丢到线程池中执行作为异步客户端。
 * 否则就是同步io，效率比较差
 * 参考：
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/asyncio.html
 * https://www.cnblogs.com/dajiangtai/p/10683664.html
 */
public class ASyncIODemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<CategoryInfo> src = env.addSource(new RichSourceFunction<CategoryInfo>() {
            @Override
            public void run(SourceContext<CategoryInfo> ctx) throws Exception {
                Long[] subId = {8109L, 4907L, 2171L, 2410L, 7769L, 3579L};
                for (Long id : subId) {
                    ctx.collect(new CategoryInfo(id, null));
                }
            }

            @Override
            public void cancel() {

            }
        });
        //方式一：同步调用+线程池
        AsyncDataStream.unorderedWait(src,new ASyncIOFunction(),1000, TimeUnit.SECONDS,10).print();
        //方式二：异步client
        AsyncDataStream.unorderedWait(src,new ASyncIOClientFunction(),1000, TimeUnit.SECONDS,10).print();
        env.execute();

    }
}
