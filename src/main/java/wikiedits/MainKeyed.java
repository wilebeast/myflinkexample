package wikiedits;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @Author liuyazhou
 * @Date 2020/4/28 22:11
 * @Version 1.0
 * @Desc
 */
public class MainKeyed {


    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> dataStream = env.
                fromElements(1L, 2L, 3L/*, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L*/)
                .keyBy(new KeySelector<Long, Long>() {
                    public Long getKey(Long key) throws Exception {
                        return key % 2;
                    }
                }).countWindow(2)//满3个元素时触发计算
                //每个窗口都会运行这个apply方法
                .apply(new WindowFunction<Long, Long, Long, GlobalWindow>() {
                    public void apply(Long key, GlobalWindow window, Iterable<Long> input, Collector<Long> out) throws Exception {
                        Iterator<Long> it = input.iterator();
                        while (it.hasNext()) {
                            out.collect(it.next() + 100 * key);
                        }//while
                    }
                });

        dataStream.print();
        //下面的execute一定要写，并且需要写在最后
        try {
            env.execute("First Flink Demo");
        } catch (
                Exception e) {
            e.printStackTrace();
        }

//        9> 101
//        9> 103
//        9> 105
//        9> 2
//        9> 4
//        9> 6
//        9> 107
//        9> 109
//        9> 111
        //8 ,10 最后是2个元素，不满3个，所以不再触发计算
    }
}
