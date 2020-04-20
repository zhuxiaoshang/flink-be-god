package operator.stream;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * connect算子，两边的数据类型可以不一样，可以共享状态(比如不基于窗口的双流join)
 * 基于ConnectedStreams的map、flatmap算子和常规map、flatmap类似
 * DataStream,DataStream → ConnectedStreams
 */
public class ConnectOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src1 = env.fromElements(Tuple3.of("k1", 11, 1587314572000L), Tuple3.of("k2", 2, 1587314574000L)
                , Tuple3.of("k3", 3, 1587314576000L), Tuple3.of("k2", 5, 1587314575000L)
                , Tuple3.of("k3", 10, 1587314577000L), Tuple3.of("k1", 9, 1587314579000L));
        DataStream<Element> src2 = env.fromElements(new Element("k4", 1587314572000L),
                new Element("k5", 1587314574000L), new Element("k1", 1587314573000L), new Element("k2",
                        1587314571000L));
        src1.connect(src2).map(new CoMapFunction<Tuple3<String, Integer, Long>, Element, Object>() {
            @Override
            public Object map1(Tuple3<String, Integer, Long> value) throws Exception {
                System.out.println("this is left :"+value);
                return value;
            }

            @Override
            public Object map2(Element value) throws Exception {
                System.out.println("this is right :"+value);
                return value;
            }
        }).print();
        env.execute();


    }

    static class Element {
        String key;
        Long timestamp;

        public Element(String key, Long timestamp) {
            this.key = key;
            this.timestamp = timestamp;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Element{" +
                    "key='" + key + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}
