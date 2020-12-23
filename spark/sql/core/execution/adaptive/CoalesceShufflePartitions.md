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
