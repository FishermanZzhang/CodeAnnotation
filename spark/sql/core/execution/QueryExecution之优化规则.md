## QE's RBO
在物理执行计划(spark plan)转成可执行物理计划(executed plan) 过程中涉及的规则优化executed plan
```
// 关联AQE 中的规则
InsertAdaptiveSparkPlan(AdaptiveExecutionContext(sparkSession, this))
// 
PlanDynamicPruningFilters(sparkSession)
PlanSubqueries(sparkSession)
EnsureRequirements(sparkSession.sessionState.conf)
ApplyColumnarRulesAndInsertTransitions(sparkSession.sessionState.conf,sparkSession.sessionState.columnarRules)
CollapseCodegenStages(sparkSession.sessionState.conf)
ReuseExchange(sparkSession.sessionState.conf)
ReuseSubquery(sparkSession.sessionState.conf)
)
```