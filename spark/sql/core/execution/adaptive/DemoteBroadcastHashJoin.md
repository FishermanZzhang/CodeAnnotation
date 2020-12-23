### Demote Broadcast Hash Joi
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
      // TODO：这里存在一个疑问
      // 如果partition 分布为[0, 0, 0, ..., 特别大的数值]
      // 此时满足条件，怎么处理？
      partitionCnt > 0 && nonZeroCnt > 0 &&
        (nonZeroCnt * 1.0 / partitionCnt) < conf.nonEmptyPartitionRatioForBroadcastJoin
    case _ => false
  }
```