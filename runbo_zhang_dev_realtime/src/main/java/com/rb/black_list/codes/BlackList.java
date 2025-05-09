package com.rb.black_list.codes;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.houbb.sensitive.word.core.SensitiveWordHelper;
import com.rb.black_list.func.FilterBloomDeduplicatorFunc;
import com.rb.black_list.func.MapCheckRedisSensitiveWordsFunc;
import com.rb.utils.CheckPointUtils;
import com.rb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @Package com.rb.black_list.codes.BlackList
 * @Author runbo.zhang
 * @Date 2025/5/7 15:59
 * @description:
 */
public class BlackList {
//    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
//    private static final String kafka_db_fact_comment_topic = ConfigUtils.getString("kafka.db.fact.comment.topic");
//    private static final String kafka_result_sensitive_words_topic = ConfigUtils.getString("kafka.result.sensitive.words.topic");
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckPointUtils.newSetCk(env, "BlackList");

        DataStreamSource<String> kafkaRead = SourceSinkUtils.kafkaRead(env, "black_list_processing_data_v1");


        SingleOutputStreamOperator<JSONObject> jsonDs = kafkaRead.map(o -> JSON.parseObject(o));

        SingleOutputStreamOperator<JSONObject> filteredDs = jsonDs.keyBy(o -> o.getLong("order_id"))
                .filter(new FilterBloomDeduplicatorFunc(1000000, 0.01));

        SingleOutputStreamOperator<JSONObject> p0ProcessDs = filteredDs.map(new MapCheckRedisSensitiveWordsFunc());
//        map.print();

        SingleOutputStreamOperator<JSONObject> p1ProcessDs = p0ProcessDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject value) throws Exception {

                int isViolation = value.getIntValue("is_violation");
                if (isViolation == 0) {//不带敏感词的数据
                    String msg = value.getString("msg");
                    List<String> all = SensitiveWordHelper.findAll(msg);
                    if (all.size() > 0  ) {//如果查询到符合SensitiveWordHelper的数据 标记p0

                        value.put("violation_grade", "P1");
                        value.put("violation_msg", String.join(", ", all));
                    }
                }
                return value;
            }
        });
        p1ProcessDs.print();


//        p1ProcessDs.map(o->o.toJSONString()).sinkTo(SourceSinkUtils.getDorisSink("doris_database_v1", "black_list_table"));



        env.disableOperatorChaining();
        env.execute();

    }
}
