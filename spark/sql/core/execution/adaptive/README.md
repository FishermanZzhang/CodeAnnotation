# AQE
adaptive-query-execution 
按照[databrick](https://databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html?from=singlemessage&isappinstalled=0https://databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html?from=singlemessage&isappinstalled=0) 的说明 AQE 包含`动态合并partition`， `动态切换join策略`， `动态优化数据倾斜`。但是把SMJ转化成BHJ和AQE是没啥关系的，反而是通过阅读代码发现了`Demote Broadcast Hash Join`

### AQE configuration
|  配置项   | 默认值 | 官方说明
|  ----  | ----  | ---- |
|spark.sql.adaptive.enabled|false|是否开启自适应查询|
|spark.sql.adaptive.coalescePartitions.enabled|true|
|spark.sql.adaptive.coalescePartitions.initialPartitionNum|none|shuffle合并分区之前的初始分区数，默认为spark.sql.shuffle.partitions的值|
|spark.sql.adaptive.coalescePartitions.minPartitionNum|none|shuffle 分区合并后的最小分区数，默认为spark集群的默认并行度|	
|spark.sql.adaptive.advisoryPartitionSizeInBytes|64MB|建议的shuffle分区的大小，在合并分区和处理join数据倾斜的时候用到	分析见|
|spark.sql.adaptive.skewJoin.enabled|true|是否开启join中数据倾斜的自适应处理|
|spark.sql.adaptive.skewJoin.skewedPartitionFactor|5|数据倾斜判断因子，必须同时满足skewedPartitionFactor和skewedPartitionThresholdInBytes|
|spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes|256MB|数据倾斜判断阈值,必须同时满足skewedPartitionFactor和skewedPartitionThresholdInBytes|
|spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin|0.2|转为broadcastJoin的非空分区比例阈值|

### Dynamically coalescing shuffle partitions
### Dynamically switching join strategies
### Dynamically optimizing skew joins
### Demote Broadcast Hash Join

### 参考文献
1. https://blog.csdn.net/zyzzxycj/article/details/106469572
2. https://databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html?from=singlemessage&isappinstalled=0
3. https://my.oschina.net/monkeyboy/blog/4768226