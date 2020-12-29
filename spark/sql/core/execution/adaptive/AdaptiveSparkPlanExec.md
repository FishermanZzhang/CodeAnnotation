1. 根节点(TreeNode)以自适应地执行查询计划. 它将查询计划分为多个独立阶段，并根据其依赖性依次执行. 查询阶段在最后物化其输出. 查询完成某个stage时，将使用物化输出的数据统计信息来优化查询的其余部分.
2. 为了创建查询阶段，我们自下而上遍历查询树. 当碰到exchange node, 并且该exchange node的所有子查询阶段物化完成(即根据物化的输出进行数据统计), 我们将为该交换节点创建一个新的查询阶段. 新的stage一旦被创建，它就会异步实现
3. 当一个查询阶段完成时，其余查询将根据所有物化阶段提供的最新统计信息进行重新优化。然后，我们再次遍历查询计划，并在可能的情况下创建更多阶段。在实现所有阶段之后，我们将执行其余计划

AQE 中RBO `CoalesceShufflePartitions`, `OptimizeSkewedJoin`, `OptimizeLocalShuffleReader`, CBO `DemoteBroadcastHashJoin` 

[源码](https://github.com/apache/spark/blob/v3.0.1/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala#L60)

```
  // 增加了DemoteBroadcastHashJoin
  // The logical plan optimizer for re-optimizing the current logical plan.
  @transient private val optimizer = new RuleExecutor[LogicalPlan] {
    // TODO add more optimization rules
    override protected def batches: Seq[Batch] = Seq(
      Batch("Demote BroadcastHashJoin", Once, DemoteBroadcastHashJoin(conf))
    )
  }

  // 三种RBO规则
  @transient private val queryStageOptimizerRules: Seq[Rule[SparkPlan]] = Seq(
    ReuseAdaptiveSubquery(conf, context.subqueryCache),
    CoalesceShufflePartitions(context.session),
    // The following two rules need to make use of 'CustomShuffleReaderExec.partitionSpecs'
    // added by `CoalesceShufflePartitions`. So they must be executed after it.
    OptimizeSkewedJoin(conf),
    OptimizeLocalShuffleReader(conf)
  )

  private def getFinalPhysicalPlan(): SparkPlan = lock.synchronized {
    if (isFinalPlan) return currentPhysicalPlan

    while(!result.allChildStagesMaterialized) {
        // 遍历stage
        // 一旦某个stage 完成, 则根据物化统计信息进行优化
        val logicalPlan = replaceWithQueryStagesInLogicalPlan(currentLogicalPlan, stagesToReplace)
        // 优化, 则到物理执行计划
        val (newPhysicalPlan, newLogicalPlan) = reOptimize(logicalPlan)
    }

    // 应用RBO
    currentPhysicalPlan = applyPhysicalRules(
      result.newPlan, queryStageOptimizerRules ++ postStageCreationRules)
    // 标志位
    isFinalPlan = true
  }

  private def reOptimize(logicalPlan: LogicalPlan): (SparkPlan, LogicalPlan) = {
    logicalPlan.invalidateStatsCache()
    // analyzed --> optimized
    val optimized = optimizer.execute(logicalPlan)
    // 物理执行计划(和可执行物理计划的区别)
    // 物理执行计划: spark plan: SparkPlan
    // 可物理执行计划: executed plan: SparkPlan
    val sparkPlan = context.session.sessionState.planner.plan(ReturnAnswer(optimized)).next()
    // RBO, CBO 优化(不包含 AQE 三个规则)
    val newPlan = applyPhysicalRules(sparkPlan, preprocessingRules ++ queryStagePreparationRules)
    (newPlan, optimized)
  }
```