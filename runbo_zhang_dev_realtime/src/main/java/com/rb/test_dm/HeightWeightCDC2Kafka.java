package com.rb.test_dm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rb.utils.CheckPointUtils;
import com.rb.utils.HbaseUtil;
import com.rb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Package com.rb.test_dm.HeightWeightCDC2Kafka
 * @Author runbo.zhang
 * @Date 2025/5/12 10:15
 * @description:
 */
public class HeightWeightCDC2Kafka {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckPointUtils.newSetCk(env, "HeightWeightCDC2Kafka");
        DataStreamSource<String> cdc = SourceSinkUtils.cdcRead(env, "online_flink_retail", "user_info_sup_msg");
//        cdc.print();
        SingleOutputStreamOperator<JSONObject> weightHeightDs = cdc.map(o -> JSON.parseObject(o));
        DataStreamSource<String> userAndOdDs = SourceSinkUtils.kafkaRead(env, "od_join_user");
        DataStreamSource<String> userKeywordDs = SourceSinkUtils.kafkaRead(env, "user_keyword");
        SingleOutputStreamOperator<JSONObject> userKeyJsonDs = userKeywordDs.map(o -> JSON.parseObject(o));
        SingleOutputStreamOperator<JSONObject> distinctDs = userAndOdDs
                .keyBy(o -> JSON.parseObject(o).getString("user_id"))
                .process(new KeyedProcessFunction<String, String, JSONObject>() {
                    ValueState<JSONObject> userState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> descriptor = new ValueStateDescriptor<>("userState", JSONObject.class);
                        userState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(String value, KeyedProcessFunction<String, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject object = JSON.parseObject(value);
                            JSONObject stateData = userState.value();
                            if (stateData == null) {
                                out.collect(object);
                            }

                        } catch (Exception e) {
                            System.err.println(e);
                        }
                        userState.update(JSON.parseObject(value));
                    }
                });
        //weightHeight数据设置水位线
        SingleOutputStreamOperator<JSONObject> wmWhDs = weightHeightDs
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts_ms");
                            }
                        }));
        //od_user数据设置水位线
        SingleOutputStreamOperator<JSONObject> wmUserDs = distinctDs
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts_ms");
                            }
                        }));
        SingleOutputStreamOperator<JSONObject> userKeyWaterDs = userKeyJsonDs
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        })
                );

        //intervalJoin 两流
        SingleOutputStreamOperator<JSONObject> od_user_weightDs = wmUserDs
                .keyBy(o -> o.getString("user_id"))
                .intervalJoin(wmWhDs.keyBy(o -> o.getJSONObject("after").getString("uid")))
                .between(Time.hours(-8), Time.hours(8))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                        JSONObject clone = (JSONObject) left.clone();
                        clone.put("weight", right.getJSONObject("after").getString("weight"));
//                        clone.put("gender", right.getJSONObject("after").getString("gender"));
                        clone.put("height", right.getJSONObject("after").getString("height"));
                        clone.put("unit_height", right.getJSONObject("after").getString("unit_height"));
                        clone.put("unit_weight", right.getJSONObject("after").getString("unit_weight"));
                        out.collect(clone);
                    }
                });
        SingleOutputStreamOperator<JSONObject> join3Ds = od_user_weightDs
                .keyBy(o -> o.getString("user_id"))
                .intervalJoin(userKeyWaterDs.keyBy(o -> o.getString("uid")))
                .between(Time.hours(-8), Time.hours(8))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject clone = (JSONObject) left.clone();
                        clone.put("os", right.getString("os"));
                        clone.put("md", right.getString("md"));
                        clone.put("vc", right.getString("vc"));
                        clone.put("ba", right.getString("ba"));
                    }
                });
        join3Ds.print();


        env.disableOperatorChaining();
        env.execute();


    }
}
