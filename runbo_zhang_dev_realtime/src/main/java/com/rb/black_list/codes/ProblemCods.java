package com.rb.black_list.codes;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.rb.black_list.utils.SensitiveWordsUtils;
import com.rb.utils.CheckPointUtils;
import com.rb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @Package com.rb.black_list.codes.ProblemCods
 * @Author runbo.zhang
 * @Date 2025/5/8 15:20
 * @description:
 */
public class ProblemCods {
    private static final ArrayList<String> sensitiveWordsLists;

    static {
        sensitiveWordsLists = SensitiveWordsUtils.getSensitiveWordsLists();
    }


    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckPointUtils.newSetCk(env, "BlackListProcessingData1");
        //todo
        DataStreamSource<String> kafkaRead = SourceSinkUtils.kafkaReadSetWater(env, "log_topic_flink_online_v1");

        SingleOutputStreamOperator<JSONObject> jsonDs = kafkaRead.map(o -> JSON.parseObject(o));





        //{"op":"c","after":{"create_time":1744123351000,"user_id":70,"appraise":"1201","comment_txt":"评论内容：57291398556419952713137891165429431798643343275826","nick_name":"伊亚","sku_id":21,"id":29,"spu_id":6,"order_id":462},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000002","connector":"mysql","pos":8928063,"name":"mysql_binlog_source","row":0,"ts_ms":1744092486000,"snapshot":"false","db":"online_flink_retail","table":"comment_info"},"ts_ms":1745225178936}
        SingleOutputStreamOperator<JSONObject> commentDs = jsonDs
                .filter(o -> "comment_info".equals(o.getJSONObject("source").getString("table")))
                .uid("commentDs").name("commentDs");
//        kafkaRead.filter(o -> "comment_info".equals(JSON.parseObject(o).getJSONObject("source").getString("table"))).print();

//        commentDs.print();
        commentDs.filter(new RichFilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                System.out.println(value.containsKey("after")+"="+value.getString("after"));
                return value.containsKey("after");
            }
        });





        env.disableOperatorChaining();
        env.execute();
    }
}
