## 数据源：
### kafka:
        dm_order_final_v2: <-od_join_user<-dws_sku_order_detail_v1
        <-dwd_trade_order_detail_v1(DwdTradeOrderDetail)
        <- log_topic_flink_online_v1_dwd(DimToHbase)
        <-log_topic_flink_online_v1(CdcSinkToKafka)

        dm_keyword_final: <-log_topic_flink_online_v1_log(flume)
### mysql:
        online_flink_retail.user_info_sup_msg

![img.png](img/dwsOrderDetail.png)
后续产生的主题
## od_join_user
![img.png](img/odJoinUser.png)
## dm_key_word
![img.png](img/dmKeyWord.png)
## dm_order_detail
![img.png](img/dmOrderDetail.png)
## dm_final_tag
![img.png](img/dmFinalTag.png)

## dorisTable
![img.png](img/dorisTable.png)


1. 数据是否写入kafka做分层 两个方案都写一下 做一下对比测试
2. 数据准确性上 把中间数据粘到文档上 去和聚合的数据做下对比 要有验证的这个过程
3. 中间的join 要写下来
4. 优化点 自己找一下优化的点在哪 之前是怎么做的 现在是怎么做的 落实到文档上
5. 数据导出到CSV文件中
