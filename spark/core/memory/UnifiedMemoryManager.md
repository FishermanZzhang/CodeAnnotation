## UnifiedMemoryManager
### yarn Resource
```
// executorMemory=spark.executor.memory
// executorOffHeapMemory=spark.memory.offHeap.size
// memoryOverhead=spark.executor.memoryOverhead 根据spark 内存管理, 这个的作用是什么?
// pysparkWorkerMemory=0 (假定不是pyspark程序)

// 资源由4部分组成
private[yarn] val resource: Resource = {
  val resource = Resource.newInstance(
    executorMemory + executorOffHeapMemory + memoryOverhead + pysparkWorkerMemory, executorCores)
  ResourceRequestHelper.setResourceRequests(executorResourceRequests, resource)
  logDebug(s"Created resource capability: $resource")
  resource
}
```

### UnifiedMemoryManager 
```
object UnifiedMemoryManager {

  // Set aside a fixed amount of memory for non-storage, non-execution purposes.

  def apply(conf: SparkConf, numCores: Int): UnifiedMemoryManager = {
    val maxMemory = getMaxMemory(conf)
    // UnifiedMemoryManager继承了 MemoryManager
    // 对heap memory 分为 storeage和executor两部分
    new UnifiedMemoryManager(
      conf,
      maxHeapMemory = maxMemory,
      onHeapStorageRegionSize =
        (maxMemory * conf.get(config.MEMORY_STORAGE_FRACTION)).toLong,
      numCores = numCores)
  }

  /**
   * Return the total amount of memory shared between execution and storage, in bytes.
   */
  private def getMaxMemory(conf: SparkConf): Long = {
    // 这个资源怎么确定?
    // 我通过实验得出 systemMemory
    val systemMemory = conf.get(TEST_MEMORY)
    // 默认300M
    val reservedMemory = RESERVED_SYSTEM_MEMORY_BYTES
    val usableMemory = systemMemory - reservedMemory
    val memoryFraction = conf.get(config.MEMORY_FRACTION) // 0.6
    (usableMemory * memoryFraction).toLong
  }
}

// 通过 acquireExecutionMemory 和 acquireStorageMemory使得内存动态调整
// UnifiedMemoryManager继承了 MemoryManager
private[spark] class UnifiedMemoryManager(
    conf: SparkConf,
    val maxHeapMemory: Long,
    onHeapStorageRegionSize: Long,
    numCores: Int)
  extends MemoryManager(
    conf,
    numCores,
    onHeapStorageRegionSize,
    maxHeapMemory - onHeapStorageRegionSize){
}


private[spark] abstract class MemoryManager(
    conf: SparkConf,
    numCores: Int,
    onHeapStorageMemory: Long,
    onHeapExecutionMemory: Long) extends Logging {

  // 初始化四个内存池
  protected val onHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.ON_HEAP)
  protected val offHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.OFF_HEAP)
  protected val onHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.ON_HEAP)
  protected val offHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.OFF_HEAP)

  // heap memory
  onHeapStorageMemoryPool.incrementPoolSize(onHeapStorageMemory)
  onHeapExecutionMemoryPool.incrementPoolSize(onHeapExecutionMemory)

  // spark.memory.offHeap.size
  // spark.memory.storageFraction=0.5
  protected[this] val maxOffHeapMemory = conf.get(MEMORY_OFFHEAP_SIZE)
  protected[this] val offHeapStorageMemory =
    (maxOffHeapMemory * conf.get(MEMORY_STORAGE_FRACTION)).toLong

  // offheap memory
  offHeapExecutionMemoryPool.incrementPoolSize(maxOffHeapMemory - offHeapStorageMemory)
  offHeapStorageMemoryPool.incrementPoolSize(offHeapStorageMemory)

  /**
   * Execution memory currently in use, in bytes.
   */
  final def executionMemoryUsed: Long = synchronized {
    onHeapExecutionMemoryPool.memoryUsed + offHeapExecutionMemoryPool.memoryUsed
  }

  /**
   * Storage memory currently in use, in bytes.
   */
  final def storageMemoryUsed: Long = synchronized {
    onHeapStorageMemoryPool.memoryUsed + offHeapStorageMemoryPool.memoryUsed
  }
} 
```

### 模拟计算
```
//假设
spark.executor.memory=5g
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=4g
spark.executor.memoryOverhead=3g
spark.memory.storageFraction=0.5 
spark.memory.fraction=0.6
//则
// 可以通过以下命令得到systemMemory
// spark-shell
// executor
// scala> spark.range(1).map(i=>Runtime.getRuntime.maxMemory / 1024.0 / 1024.0 / 1024.0).collect
// res2: Array[Double] = Array(4.44482421875)

systemMemory=4.44482421875g 
reservedMemory=300M
maxMemory=(systemMemory - 300/1024)*0.6 = 2.49111328125
在heap 上为非存储，非执行目的预留一定数量的内存=(systemMemory - 300M )*0.4
onHeapStorageMemory=maxMemory * 0.5
onHeapExecution=maxMemory * (1 - 0.5)
maxOffHeapMemory=4g // spark.memory.offHeap.size
offHeapStorageMemory=maxOffHeapMemory * 0.5
offHeapExecutionMemory=maxOffHeapMemory * (1 - 0.5)
```

`spark-ui` 上显示的`Storage Memory` 为 `执行内存` + `存贮内存`. 本实验中为通过计算为 `6.49111328125`, `spark-ui` 显示 `6.5g`. 

### 疑问
* spark.executor.memoryOverhead  这个参数有什么用?
* spark.testing.memory 与 spark.memory.offHeap.size 的关系?

### 参考
1. https://www.turbofei.wang/spark/2016/12/19/spark%E7%BB%9F%E4%B8%80%E5%86%85%E5%AD%98%E7%AE%A1%E7%90%86
2. https://www.jianshu.com/p/911a5fc967c5
3. https://developpaper.com/spark-unified-memory-management/