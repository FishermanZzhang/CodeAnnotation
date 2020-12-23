## Dynamically optimizing skew joins
### no aqe vs aqe
假设 partition0的数据远远多余其他分区. 开启aqe 后,则会对skewed partition 重新划分. 例如partition0 就会划分成2块.
![no aqe](https://databricks.com/wp-content/uploads/2020/05/blog-adaptive-query-execution-5.png)
![aqe](https://databricks.com/wp-content/uploads/2020/05/blog-adaptive-query-execution-6.png)

### 注解

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