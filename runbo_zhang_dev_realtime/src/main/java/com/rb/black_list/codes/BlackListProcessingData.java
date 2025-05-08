package com.rb.black_list.codes;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rb.black_list.utils.SensitiveWordsUtils;
import com.rb.black_list.utils.siliconflow.CommonGenerateTempLate;
import com.rb.fuction.DimAsync;
import com.rb.utils.CheckPointUtils;

import com.rb.utils.DateFormatUtil;
import com.rb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.rb.black_list.codes.Processing_Data
 * @Author runbo.zhang
 * @Date 2025/5/7 16:24
 * @description:
 */
public class BlackListProcessingData {
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

        SingleOutputStreamOperator<JSONObject> after = jsonDs.filter(new RichFilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {

                return false;
            }
        });

        SingleOutputStreamOperator<JSONObject> OrderInfoDs = jsonDs
                .filter(o -> "order_info".equals(o.getJSONObject("source").getString("table")) && o.containsKey("after"))
                .uid("OrderInfoDs").name("OrderInfoDs");


        //{"op":"c","after":{"create_time":1744123351000,"user_id":70,"appraise":"1201","comment_txt":"评论内容：57291398556419952713137891165429431798643343275826","nick_name":"伊亚","sku_id":21,"id":29,"spu_id":6,"order_id":462},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000002","connector":"mysql","pos":8928063,"name":"mysql_binlog_source","row":0,"ts_ms":1744092486000,"snapshot":"false","db":"online_flink_retail","table":"comment_info"},"ts_ms":1745225178936}
        SingleOutputStreamOperator<JSONObject> commentDs = jsonDs
                .filter(o -> "comment_info".equals(o.getJSONObject("source").getString("table")))
                .uid("commentDs").name("commentDs");
//        commentDs.print();
//        commentDs.filter(new RichFilterFunction<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject value) throws Exception {
//                System.out.println(value.containsKey("after")+"="+value.getString("after"));
//                return value.containsKey("after");
//            }
//        });


        DataStream<JSONObject> keyedStream = commentDs.filter(o -> !StringUtils.isEmpty(o.getString("after"))).keyBy(o -> o.getJSONObject("after").getString("appraise"));

//        keyedStream.print();

        //{"op":"c","after":{"create_time":1744204944000,"user_id":118,"appraise":"1201","comment_txt":"评论内容：27886673813985174491147526819254467889267492463873","nick_name":"巧美","sku_id":33,"id":49,"spu_id":11,"order_id":632},"source":{"server_id":1,"version":"1.6.4.Final","file":"mysql-bin.000002","connector":"mysql","pos":11399512,"name":"mysql_binlog_source","row":0,"ts_ms":1744092543000,"snapshot":"false","db":"online_flink_retail","table":"comment_info"},"ts_ms":1745225182409}
        SingleOutputStreamOperator<JSONObject> asyncDs = AsyncDataStream.unorderedWait(keyedStream, new DimAsync<JSONObject>() {
            @Override
            public void addDims(JSONObject obj, JSONObject dimJsonObj) {
                obj.put("dic_name", dimJsonObj.getString("dic_name"));
            }

            @Override
            public String getTableName() {
                return "dim_base_dic";
            }

            @Override
            public String getRowKey(JSONObject obj) {
                return obj.getJSONObject("after").getString("appraise");
            }
        }, 100, TimeUnit.SECONDS).uid("asyncDs").name("asyncDs");
        //asyncDs.print();
        SingleOutputStreamOperator<JSONObject> resComDs = asyncDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject resJsonObj = new JSONObject();
                Long tsMs = jsonObject.getLong("ts_ms");
                JSONObject source = jsonObject.getJSONObject("source");
                String dbName = source.getString("db");
                String tableName = source.getString("table");
                String serverId = source.getString("server_id");
                if (!StringUtils.isEmpty(jsonObject.getString("after"))) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    resJsonObj.put("ts_ms", tsMs);
                    resJsonObj.put("db", dbName);
                    resJsonObj.put("table", tableName);
                    resJsonObj.put("server_id", serverId);
                    resJsonObj.put("appraise", after.getString("appraise"));
                    resJsonObj.put("commentTxt", after.getString("comment_txt"));
                    resJsonObj.put("op", jsonObject.getString("op"));
                    resJsonObj.put("nick_name", jsonObject.getString("nick_name"));
                    resJsonObj.put("create_time", after.getLong("create_time"));
                    resJsonObj.put("user_id", after.getLong("user_id"));
                    resJsonObj.put("sku_id", after.getLong("sku_id"));
                    resJsonObj.put("id", after.getLong("id"));
                    resJsonObj.put("spu_id", after.getLong("spu_id"));
                    resJsonObj.put("order_id", after.getLong("order_id"));
                    resJsonObj.put("dic_name", jsonObject.getString("dic_name"));
                    return resJsonObj;
                }
                return null;
            }
        }).uid("resComDs").name("resComDs");
//        resComDs.print();
        SingleOutputStreamOperator<JSONObject> resOrderDs = OrderInfoDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject inputJsonObj) throws Exception {
                String op = inputJsonObj.getString("op");
                long tm_ms = inputJsonObj.getLongValue("ts_ms");
                JSONObject dataObj;
                if (inputJsonObj.containsKey("after") && !StringUtils.isEmpty(inputJsonObj.getString("after"))) {
                    dataObj = inputJsonObj.getJSONObject("after");
                } else {
                    dataObj = inputJsonObj.getJSONObject("before");
                }
                JSONObject resultObj = new JSONObject();
                resultObj.put("op", op);
                resultObj.put("tm_ms", tm_ms);
                resultObj.putAll(dataObj);
                return resultObj;
            }
        }).uid("resOrderDs").name("resOrderDs");

        KeyedStream<JSONObject, String> keyedComDs = resComDs.keyBy(o -> o.getString("order_id"));
        KeyedStream<JSONObject, String> keyedOrderDs = resOrderDs.keyBy(o -> o.getString("id"));
        SingleOutputStreamOperator<JSONObject> joinOiCoDs = keyedComDs
                .intervalJoin(keyedOrderDs)
                .between(Time.minutes(-1), Time.minutes(1))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                        //评论数据关联order_info表 给order_info的字段加上info_前缀
                        JSONObject clone = (JSONObject) left.clone();
                        for (String s : right.keySet()) {
                            clone.put("info_" + s, right.getString(s));
                        }
                        out.collect(clone);
                    }
                }).uid("joinOiCoDs").name("joinOiCoDs");
//        joinOiCoDs.print();
        SingleOutputStreamOperator<JSONObject> badComDs = joinOiCoDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            private transient Random random;

            @Override
            public void open(Configuration parameters) {
                random = new Random();
            }
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                if (random.nextDouble() < 0.5){
                    jsonObject.put("dic_name","差评");
                }
                //调用ai接口生成差评
                jsonObject.put("commentTxt", CommonGenerateTempLate.GenerateComment(jsonObject.getString("dic_name"), jsonObject.getString("info_trade_body")));
                return jsonObject;
            }
        }).uid("badComDs").name("badComDs");
        SingleOutputStreamOperator<JSONObject> sensitiveDs = badComDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            private transient Random random;

            @Override
            public void open(Configuration parameters) {
                random = new Random();
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                if (random.nextDouble() < 0.2) {
                    //调用本地的敏感词文档，给20%数据添加敏感词
                    jsonObject.put("commentTxt", jsonObject.getString("commentTxt") + "," + SensitiveWordsUtils.getRandomElement(sensitiveWordsLists));
                    System.err.println("change commentTxt: " + jsonObject);
                }
                return jsonObject;
            }
        }).uid("sensitiveDs").name("sensitiveDs");

        SingleOutputStreamOperator<JSONObject> finalDs = sensitiveDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                jsonObject.put("ds", DateFormatUtil.tsToDate(jsonObject.getLong("ts_ms")));
                return jsonObject;

            }
        }).uid("finalDs").name("finalDs");

        finalDs.map(o->o.toJSONString()).sinkTo(SourceSinkUtils.sinkToKafka("black_list_processing_data_v1"));


        env.disableOperatorChaining();
        env.execute();

    }


}
