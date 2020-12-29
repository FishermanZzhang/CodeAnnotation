## QE's RBO
在物理执行计划(spark plan)转成可执行物理计划(executed plan) 过程中涉及的规则优化

[源码](https://github.com/apache/spark/blob/v3.0.1/sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala#L296)
### 1. InsertAdaptiveSparkPlan
[关联AQE 中的规则](./adaptive/AdaptiveSparkPlanExec.md) 

// TODO 搞明白一下规则的原理

### 2. PlanDynamicPruningFilters

### 3. PlanSubqueries

### 4. EnsureRequirements

### 5. ApplyColumnarRulesAndInsertTransitions

### 6. CollapseCodegenStages

### 7. ReuseExchange

### 8. 7. ReuseSubquery
