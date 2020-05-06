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
        DataStreamSource<StoreInfo> src = env.addSource(new RichSourceFunction<StoreInfo>() {
            @Override
            public void run(SourceContext<StoreInfo> ctx) throws Exception {
                String[] cds = {"076", "180873434", "183286343", "18328643", "18435643", "74MV"};
                for (String cd : cds) {
                    ctx.collect(new StoreInfo(cd, ""));
                }
            }

            @Override
            public void cancel() {

            }
        });
        //AsyncDataStream.unorderedWait(src,new ASyncIOFunction(),1000, TimeUnit.SECONDS,10).print();
        AsyncDataStream.unorderedWait(src,new ASyncIOClientFunction(),1000, TimeUnit.SECONDS,10).print();
        env.execute();

    }
}
