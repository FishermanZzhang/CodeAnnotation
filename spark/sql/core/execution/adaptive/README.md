# AQE 
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
[原始代码](https://github.com/apache/spark/blob/v3.0.1/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/CoalesceShufflePartitions.scala#L33)
```
  override def apply(plan: SparkPlan): SparkPlan = {
    // spark.sql.adaptive.enabled 默认为false
    if (!conf.coalesceShufflePartitionsEnabled) {
      return plan
    }

    // 查询依赖stage 的运行状态, 注释没看懂
    if (!plan.collectLeaves().forall(_.isInstanceOf[QueryStageExec])
      || plan.find(_.isInstanceOf[CustomShuffleReaderExec]).isDefined) {
      // If not all leaf nodes are query stages, it's not safe to reduce the number of
      // shuffle partitions, because we may break the assumption that all children of a spark plan
      // have same number of output partitions.
      return plan
    }

    def collectShuffleStages(plan: SparkPlan): Seq[ShuffleQueryStageExec] = plan match {
      case stage: ShuffleQueryStageExec => Seq(stage)
      case _ => plan.children.flatMap(collectShuffleStages)
    }

    val shuffleStages = collectShuffleStages(plan)
    // ShuffleExchanges introduced by repartition do not support changing the number of partitions.
    // We change the number of partitions in the stage only if all the ShuffleExchanges support it.
    // 如果用户指定了分区操作，如repartition等，则跳过优化
    if (!shuffleStages.forall(_.shuffle.canChangeNumPartitions)) {
      plan
    } else {
      // `ShuffleQueryStageExec#mapStats` returns None when the input RDD has 0 partitions,
      // we should skip it when calculating the `partitionStartIndices`.

      // 统计信息，如每个shuffle write 的每个partition的字节数
      val validMetrics = shuffleStages.flatMap(_.mapStats)

      // We may have different pre-shuffle partition numbers, don't reduce shuffle partition number
      // in that case. For example when we union fully aggregated data (data is arranged to a single
      // partition) and a result of a SortMergeJoin (multiple partitions).
      val distinctNumPreShufflePartitions =
      validMetrics.map(stats => stats.bytesByPartitionId.length).distinct
      // 获取每个task的分区数，如果一致则进入优化
      if (validMetrics.nonEmpty && distinctNumPreShufflePartitions.length == 1) {
        // We fall back to Spark default parallelism if the minimum number of coalesced partitions
        // is not set, so to avoid perf regressions compared to no coalescing.
        val minPartitionNum = conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM)
          .getOrElse(session.sparkContext.defaultParallelism)
        // coalesce 方法, 根据规则线段合并，返回合并后的分区区间
        // spark.sql.adaptive.advisoryPartitionSizeInBytes 默认64m
        // spark.sql.adaptive.coalescePartitions.minPartitionNum
        // shuffle 分区合并后的最小分区数，默认为spark集群的默认并行度
        val partitionSpecs = ShufflePartitionsUtil.coalescePartitions(
          validMetrics.toArray,
          advisoryTargetSize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES),
          minNumPartitions = minPartitionNum)
        // This transformation adds new nodes, so we must use `transformUp` here.
        val stageIds = shuffleStages.map(_.id).toSet
        plan.transformUp {
          // even for shuffle exchange whose input RDD has 0 partition, we should still update its
          // `partitionStartIndices`, so that all the leaf shuffles in a stage have the same
          // number of output partitions.
          case stage: ShuffleQueryStageExec if stageIds.contains(stage.id) =>
            CustomShuffleReaderExec(stage, partitionSpecs, COALESCED_SHUFFLE_READER_DESCRIPTION)
        }
      } else {
        plan
      }
    }
  }

  // 合并分区逻辑
  def coalescePartitions(
      mapOutputStatistics: Array[MapOutputStatistics],
      advisoryTargetSize: Long,
      minNumPartitions: Int): Seq[ShufflePartitionSpec] = {
    // If `minNumPartitions` is very large, it is possible that we need to use a value less than
    // `advisoryTargetSize` as the target size of a coalesced task.

    // 获取总的输入大小
    val totalPostShuffleInputSize = mapOutputStatistics.map(_.bytesByPartitionId.sum).sum
    // The max at here is to make sure that when we have an empty table, we only have a single
    // coalesced partition.
    // There is no particular reason that we pick 16. We just need a number to prevent
    // `maxTargetSize` from being set to 0.

    // 判断targetSize 大小的方法
    // 用户配置 minPartitionNum和advisoryTargetSize 
    val maxTargetSize = math.max(
      math.ceil(totalPostShuffleInputSize / minNumPartitions.toDouble).toLong, 16)
    val targetSize = math.min(maxTargetSize, advisoryTargetSize)

    val shuffleIds = mapOutputStatistics.map(_.shuffleId).mkString(", ")
    logInfo(s"For shuffle($shuffleIds), advisory target size: $advisoryTargetSize, " +
      s"actual target size $targetSize.")

    // Make sure these shuffles have the same number of partitions.
    val distinctNumShufflePartitions =
      mapOutputStatistics.map(stats => stats.bytesByPartitionId.length).distinct
    // The reason that we are expecting a single value of the number of shuffle partitions
    // is that when we add Exchanges, we set the number of shuffle partitions
    // (i.e. map output partitions) using a static setting, which is the value of
    // `spark.sql.shuffle.partitions`. Even if two input RDDs are having different
    // number of partitions, they will have the same number of shuffle partitions
    // (i.e. map output partitions).
    assert(
      distinctNumShufflePartitions.length == 1,
      "There should be only one distinct value of the number of shuffle partitions " +
        "among registered Exchange operators.")

    val numPartitions = distinctNumShufflePartitions.head
    // 使用partitionSpecs 保存最终结果，线段形式
    val partitionSpecs = ArrayBuffer[CoalescedPartitionSpec]()
    var latestSplitPoint = 0
    var coalescedSize = 0L
    var i = 0
    // 循环判断当前分区和targetSize 的关系
    // 这里可以看出来 mapOutputStatistics 是二位矩阵，所以
    //   (1) 所有shuffle write的分区数必须一致
    //   (2) 只能处理连续的分区
    // 可以对分区总量进行排序，再做合并，就能解决多个不连续的小分区合并问题
    while (i < numPartitions) {
      // We calculate the total size of i-th shuffle partitions from all shuffles.
      var totalSizeOfCurrentPartition = 0L
      var j = 0
      while (j < mapOutputStatistics.length) {
        totalSizeOfCurrentPartition += mapOutputStatistics(j).bytesByPartitionId(i)
        j += 1
      }

      // If including the `totalSizeOfCurrentPartition` would exceed the target size, then start a
      // new coalesced partition.
      if (i > latestSplitPoint && coalescedSize + totalSizeOfCurrentPartition > targetSize) {
        partitionSpecs += CoalescedPartitionSpec(latestSplitPoint, i)
        latestSplitPoint = i
        // reset postShuffleInputSize.
        coalescedSize = totalSizeOfCurrentPartition
      } else {
        coalescedSize += totalSizeOfCurrentPartition
      }
      i += 1
    }
    partitionSpecs += CoalescedPartitionSpec(latestSplitPoint, numPartitions)

    partitionSpecs
  }
```

### Dynamically switching join strategies
### Dynamically optimizing skew joins
只有在`join`是才做此优化，
在没有`join`的时候也会出现和`动态合并分区`相反的情况，即某个分区的部分shuffle write特别大，为什么没有做这种优化呢？

[原始代码](https://github.com/apache/spark/blob/v3.0.1/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/OptimizeSkewedJoin.scala#L156) 函数入口依然为` override def apply(plan: SparkPlan)`此处忽略一些细节和调用栈
```
  // 只有两个表smj时才做优化。（多表join使用语法树解析后也是多个2表join吧）
  val shuffleStages = collectShuffleStages(plan)
  if (shuffleStages.length == 2) {
    // When multi table join, there will be too many complex combination to consider.
    // Currently we only handle 2 table join like following use case.
    // SMJ
    //   Sort
    //     Shuffle
    //   Sort
    //     Shuffle
    val optimizePlan = optimizeSkewJoin(plan))
    // ....
  }
  
  // 先看三个小函数
  // spark.sql.adaptive.skewJoin.skewedPartitionFactor 默认5
  // spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes 默认256M
  private def isSkewed(size: Long, medianSize: Long): Boolean = {
    size > medianSize * conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR) &&
      size > conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD)
  }

  private def medianSize(sizes: Seq[Long]): Long = {
    val numPartitions = sizes.length
    val bytes = sizes.sorted
    numPartitions match {
      case _ if (numPartitions % 2 == 0) =>
        math.max((bytes(numPartitions / 2) + bytes(numPartitions / 2 - 1)) / 2, 1)
      case _ => math.max(bytes(numPartitions / 2), 1)
    }
  }

  /**
   * The goal of skew join optimization is to make the data distribution more even. The target size
   * to split skewed partitions is the average size of non-skewed partition, or the
   * advisory partition size if avg size is smaller than it.
   */
  private def targetSize(sizes: Seq[Long], medianSize: Long): Long = {
    // targetsize计算过程：剔除掉skewed，再计算平均值
    val advisorySize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    // 判断skewed 标准为：使用分区的中位数,计算是否skew
    val nonSkewSizes = sizes.filterNot(isSkewed(_, medianSize))
    // It's impossible that all the partitions are skewed, as we use median size to define skew.
    assert(nonSkewSizes.nonEmpty)
    // 剔除skewed, 在和 advisorysize 比较。
    math.max(advisorySize, nonSkewSizes.sum / nonSkewSizes.length)
  }

  /*
   * This method aim to optimize the skewed join with the following steps:
   * 1. Check whether the shuffle partition is skewed based on the median size
   *    and the skewed partition threshold in origin smj.
   * 2. Assuming partition0 is skewed in left side, and it has 5 mappers (Map0, Map1...Map4).
   *    And we may split the 5 Mappers into 3 mapper ranges [(Map0, Map1), (Map2, Map3), (Map4)]
   *    based on the map size and the max split number.
   * 3. Wrap the join left child with a special shuffle reader that reads each mapper range with one
   *    task, so total 3 tasks.
   * 4. Wrap the join right child with a special shuffle reader that reads partition0 3 times by
   *    3 tasks separately.
   */
  // 先看注释，再看代码
  def optimizeSkewJoin(plan: SparkPlan): SparkPlan = plan.transformUp {
    case smj @ SortMergeJoinExec(_, _, joinType, _,
        s1 @ SortExec(_, _, ShuffleStage(left: ShuffleStageInfo), _),
        s2 @ SortExec(_, _, ShuffleStage(right: ShuffleStageInfo), _), _)
        if supportedJoinTypes.contains(joinType) =>
      assert(left.partitionsWithSizes.length == right.partitionsWithSizes.length) // 之前也没有判断left 分区个数和 right 分区个数相等， 为啥直接用的assert
      // partitionsWithSize的第二项，记录每个分区的大小
      // 假设：
      //             p1    p2   p3
      //   shuffle1 [100, 200, 300]
      //   shuffle2 [5000, 200, 50]
      //   numPartitions=3
      // 所以：
      //   partitionsWithSizes的第二个值=[5100, 400, 350]
      //   中位数=400
      //   partiton1 是倾斜的,计算平均值时剔除
      //   平均值=375.0 (400 + 350) / 2
      //   targetSize=375.0 max(advisorySize=256M, 平均值)
      //   那么p1 就会划分成两个partition

      // 获取分区个数
      val numPartitions = left.partitionsWithSizes.length
      // Use the median size of the actual (coalesced) partition sizes to detect skewed partitions.
      // 根据每个partition分布，计算中位数
      val leftMedSize = medianSize(left.partitionsWithSizes.map(_._2))
      val rightMedSize = medianSize(right.partitionsWithSizes.map(_._2))
      // 根据join类型判断是否可以spilt
      val canSplitLeft = canSplitLeftSide(joinType)
      val canSplitRight = canSplitRightSide(joinType)
      // We use the actual partition sizes (may be coalesced) to calculate target size, so that
      // the final data distribution is even (coalesced partitions + split partitions).
      val leftActualSizes = left.partitionsWithSizes.map(_._2)
      val rightActualSizes = right.partitionsWithSizes.map(_._2)
      // 计算targetsize
      val leftTargetSize = targetSize(leftActualSizes, leftMedSize)
      val rightTargetSize = targetSize(rightActualSizes, rightMedSize)

      val leftSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
      val rightSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
      val leftSkewDesc = new SkewDesc
      val rightSkewDesc = new SkewDesc
      // 遍历每个partition，计算当前partition划分个数。
      for (partitionIndex <- 0 until numPartitions) {
        val isLeftSkew = isSkewed(leftActualSizes(partitionIndex), leftMedSize) && canSplitLeft
        val leftPartSpec = left.partitionsWithSizes(partitionIndex)._1
        val isLeftCoalesced = leftPartSpec.startReducerIndex + 1 < leftPartSpec.endReducerIndex

        val isRightSkew = isSkewed(rightActualSizes(partitionIndex), rightMedSize) && canSplitRight
        val rightPartSpec = right.partitionsWithSizes(partitionIndex)._1
        val isRightCoalesced = rightPartSpec.startReducerIndex + 1 < rightPartSpec.endReducerIndex

        // A skewed partition should never be coalesced, but skip it here just to be safe.
        val leftParts = if (isLeftSkew && !isLeftCoalesced) {
          val reducerId = leftPartSpec.startReducerIndex
          // 针对同一个partition做划分，后续会详细说明
          val skewSpecs = createSkewPartitionSpecs(
            left.mapStats.shuffleId, reducerId, leftTargetSize)
          if (skewSpecs.isDefined) {
            logDebug(s"Left side partition $partitionIndex is skewed, split it into " +
              s"${skewSpecs.get.length} parts.")
            leftSkewDesc.addPartitionSize(leftActualSizes(partitionIndex))
          }
          skewSpecs.getOrElse(Seq(leftPartSpec))
        } else {
          Seq(leftPartSpec)
        }

        // A skewed partition should never be coalesced, but skip it here just to be safe.
        val rightParts = if (isRightSkew && !isRightCoalesced) {
          val reducerId = rightPartSpec.startReducerIndex
          val skewSpecs = createSkewPartitionSpecs(
            right.mapStats.shuffleId, reducerId, rightTargetSize)
          if (skewSpecs.isDefined) {
            logDebug(s"Right side partition $partitionIndex is skewed, split it into " +
              s"${skewSpecs.get.length} parts.")
            rightSkewDesc.addPartitionSize(rightActualSizes(partitionIndex))
          }
          skewSpecs.getOrElse(Seq(rightPartSpec))
        } else {
          Seq(rightPartSpec)
        }

        // 笛卡尔积
        for {
          leftSidePartition <- leftParts
          rightSidePartition <- rightParts
        } {
          leftSidePartitions += leftSidePartition
          rightSidePartitions += rightSidePartition
        }
      }
  }
  // createSkewPartitionSpecs --> ShufflePartitionsUtil.splitSizeListByTargetSize
  // 变量 sizes 为同一个partiton不同shuffle write的长度 
  // 返回值为数组，array再转换为PartitionSpecs得到此partition的划分情况
  def splitSizeListByTargetSize(sizes: Seq[Long], targetSize: Long): Array[Int] = {
    val partitionStartIndices = ArrayBuffer[Int]()
    partitionStartIndices += 0
    var i = 0
    var currentPartitionSize = 0L
    var lastPartitionSize = -1L

    def tryMergePartitions() = {
      // When we are going to start a new partition, it's possible that the current partition or
      // the previous partition is very small and it's better to merge the current partition into
      // the previous partition.
      // 当前 partition 小 很好理解。当前的partition 小 可能是后边的partition 比较大 或者 是最后一个partition 了。
      // 那么pre partition 小怎么出现？
      val shouldMergePartitions = lastPartitionSize > -1 &&
        ((currentPartitionSize + lastPartitionSize) < targetSize * MERGED_PARTITION_FACTOR ||
        (currentPartitionSize < targetSize * SMALL_PARTITION_FACTOR ||
          lastPartitionSize < targetSize * SMALL_PARTITION_FACTOR))
      if (shouldMergePartitions) {
        // We decide to merge the current partition into the previous one, so the start index of
        // the current partition should be removed.
        partitionStartIndices.remove(partitionStartIndices.length - 1)
        lastPartitionSize += currentPartitionSize
      } else {
        lastPartitionSize = currentPartitionSize
      }
    }

    while (i < sizes.length) {
      // If including the next size in the current partition exceeds the target size, package the
      // current partition and start a new partition.
      // currentPartitionSize为shuffle write 的累计值
      if (i > 0 && currentPartitionSize + sizes(i) > targetSize) {
        tryMergePartitions()
        partitionStartIndices += i
        currentPartitionSize = sizes(i)
      } else {
        currentPartitionSize += sizes(i)
      }
      i += 1
    }
    tryMergePartitions()
    partitionStartIndices.toArray
  }
```

### Demote Broadcast Hash Join
还不知道这个优化的应用场景

执行过程为：AdaptiveSparkPlanExec.scala
```
  @transient private val optimizer = new RuleExecutor[LogicalPlan] {
    // TODO add more optimization rules
    override protected def batches: Seq[Batch] = Seq(
      Batch("Demote BroadcastHashJoin", Once, DemoteBroadcastHashJoin(conf))
    )
  }
  private def getFinalPhysicalPlan(): SparkPlan = lock.synchronized {
    // 忽略一些细节
    // reOptimize 执行Demote Broadcast Hash Join， 返回新的执行计划
    val (newPhysicalPlan, newLogicalPlan) = reOptimize(logicalPlan)
    // 比较执行计划cost。 CBO
    val origCost = costEvaluator.evaluateCost(currentPhysicalPlan)
    val newCost = costEvaluator.evaluateCost(newPhysicalPlan)
  }
```
[原始代码](https://github.com/apache/spark/blob/v3.0.1/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/DemoteBroadcastHashJoin.scala#L41), 通过apply进入`shouldDemote` 
```
  private def shouldDemote(plan: LogicalPlan): Boolean = plan match {
    case LogicalQueryStage(_, stage: ShuffleQueryStageExec) if stage.resultOption.isDefined
      && stage.mapStats.isDefined =>
      val mapStats = stage.mapStats.get
      // partition 个数
      val partitionCnt = mapStats.bytesByPartitionId.length
      // 有效的partition 个数
      val nonZeroCnt = mapStats.bytesByPartitionId.count(_ > 0)
      // spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin 默认0.2
      partitionCnt > 0 && nonZeroCnt > 0 &&
        (nonZeroCnt * 1.0 / partitionCnt) < conf.nonEmptyPartitionRatioForBroadcastJoin
    case _ => false
  }
```


### 参考文献
1. https://blog.csdn.net/zyzzxycj/article/details/106469572
2. https://databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html?from=singlemessage&isappinstalled=0
3. https://my.oschina.net/monkeyboy/blog/4768226