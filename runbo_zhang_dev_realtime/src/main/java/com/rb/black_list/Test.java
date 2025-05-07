package com.rb.black_list;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Collection;

/**
 * @Package com.rb.black_list.Test
 * @Author runbo.zhang
 * @Date 2025/5/7 21:20
 * @description:
 */
public class Test {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                ctx.collect("{\"info_original_total_amount\":\"KpQ=\",\"info_activity_reduce_amount\":\"AA==\",\"commentTxt\":\"商品质量一般，米粒太小，米壳太重。,中华人民实话实说\",\"info_province_id\":26,\"info_payment_way\":\"3501\",\"info_create_time\":1743247225000,\"info_refundable_time\":1743852025000,\"info_order_status\":\"1001\",\"id\":2280,\"spu_id\":10,\"table\":\"comment_info\",\"info_tm_ms\":1743232779070,\"op\":\"c\",\"create_time\":1743247288000,\"info_user_id\":4415,\"info_op\":\"c\",\"info_trade_body\":\"十月稻田 辽河长粒香 东北大米 5kg等2件商品\",\"sku_id\":30,\"server_id\":\"1\",\"dic_name\":\"好评\",\"info_consignee_tel\":\"13153161128\",\"info_total_amount\":\"KpQ=\",\"info_out_trade_no\":\"167898577211957\",\"appraise\":\"1201\",\"user_id\":4415,\"info_id\":23169,\"info_coupon_reduce_amount\":\"AA==\",\"order_id\":23169,\"info_consignee\":\"钱霭\",\"ts_ms\":1743232779410,\"db\":\"realtime_v1\"}\n");
                ctx.collect("{\"aaaaa\":\"G7e0\",\"info_activity_reduce_amount\":\"Yag=\",\"commentTxt\":\"联想（Lenovo） 拯救者Y9000P 2022 16英寸游戏笔记本电脑 i9-12900H RTX3060 钛晶灰：垃圾机器！发热离谱！屏幕也太差了！绝对破东西！最坑的品牌！,一品楼VIP高级帐号\",\"info_province_id\":33,\"info_payment_way\":\"3501\",\"info_create_time\":1743247699000,\"info_refundable_time\":1743852499000,\"info_order_status\":\"1001\",\"id\":2282,\"spu_id\":10,\"table\":\"comment_info\",\"info_tm_ms\":1743232779145,\"op\":\"c\",\"create_time\":1743247761000,\"info_user_id\":4507,\"info_op\":\"c\",\"info_trade_body\":\"联想（Lenovo） 拯救者Y9000P 2022 16英寸游戏笔记本电脑 i9-12900H RTX3060 钛晶灰等3件商品\",\"sku_id\":30,\"server_id\":\"1\",\"dic_name\":\"差评\",\"info_consignee_tel\":\"13525643292\",\"info_total_amount\":\"G1YM\",\"info_out_trade_no\":\"778925799748712\",\"appraise\":\"1203\",\"user_id\":4507,\"info_id\":23170,\"info_coupon_reduce_amount\":\"AA==\",\"order_id\":23170,\"info_consignee\":\"戚柔竹\",\"ts_ms\":1743232779499,\"db\":\"realtime_v1\"}\n");
                ctx.collect("{\"info_original_total_amount\":\"HBGM\",\"info_activity_reduce_amount\":\"AWdW\",\"commentTxt\":\"购买TCL 75Q10电视后要求退货却被告知需支付额外退换费；售后客服处理问题态度生硬；电视屏幕效果不佳发黄严重且对比度不足。,万维读者论坛\",\"info_province_id\":9,\"info_payment_way\":\"3501\",\"info_create_time\":1743262402000,\"info_refundable_time\":1743867202000,\"info_order_status\":\"1002\",\"id\":2292,\"spu_id\":5,\"table\":\"comment_info\",\"info_tm_ms\":1743232781253,\"info_operate_time\":1743262432000,\"op\":\"c\",\"create_time\":1743262460000,\"info_user_id\":2632,\"info_op\":\"u\",\"info_trade_body\":\"TCL 75Q10 75英寸 QLED原色量子点电视 安桥音响 AI声控智慧屏 超薄全面屏 MEMC防抖 3+32GB 平板电视等3件商品\",\"sku_id\":18,\"server_id\":\"1\",\"dic_name\":\"中评\",\"info_consignee_tel\":\"13994279395\",\"info_total_amount\":\"Gqo2\",\"info_out_trade_no\":\"558719565596956\",\"appraise\":\"1202\",\"user_id\":2632,\"info_id\":23216,\"info_coupon_reduce_amount\":\"AA==\",\"order_id\":23216,\"info_consignee\":\"祁达安\",\"ts_ms\":1743232781371,\"db\":\"realtime_v1\"}\n");
                ctx.collect("{\"info_original_total_amount\":\"HBGM\",\"info_activity_reduce_amount\":\"AWdW\",\"commentTxt\":\"购买TCL 75Q10电视后要求退货却被告知需支付额外退换费；售后客服处理问题态度生硬；电视屏幕效果不佳发黄严重且对比度不足。,万维读者论坛\",\"info_province_id\":9,\"info_payment_way\":\"3501\",\"info_create_time\":1743262402000,\"info_refundable_time\":1743867202000,\"info_order_status\":\"1002\",\"id\":2292,\"spu_id\":5,\"table\":\"comment_info\",\"info_tm_ms\":1743232781253,\"info_operate_time\":1743262432000,\"op\":\"c\",\"create_time\":1743262460000,\"info_user_id\":2632,\"info_op\":\"u\",\"info_trade_body\":\"TCL 75Q10 75英寸 QLED原色量子点电视 安桥音响 AI声控智慧屏 超薄全面屏 MEMC防抖 3+32GB 平板电视等3件商品\",\"sku_id\":18,\"server_id\":\"1\",\"dic_name\":\"中评\",\"info_consignee_tel\":\"13994279395\",\"info_total_amount\":\"Gqo2\",\"info_out_trade_no\":\"558719565596956\",\"appraise\":\"1202\",\"user_id\":2632,\"info_id\":23216,\"info_coupon_reduce_amount\":\"AA==\",\"order_id\":23216,\"info_consignee\":\"祁达安\",\"ts_ms\":1743232781371,\"db\":\"realtime_v1\"}\n");

            }

            @Override
            public void cancel() {

            }
        });
        SingleOutputStreamOperator<JSONObject> jsonDs = ds.map(o -> JSON.parseObject(o));
        SingleOutputStreamOperator<JSONObject> filter = jsonDs.filter(new RichFilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                System.out.println(value + "包含aaaa");
                System.out.println(value.containsKey("aaaaa"));

                return value.containsKey("aaaaa");
            }
        });
        filter.print("ffffff");


        env.disableOperatorChaining();
        env.execute();
    }
}
