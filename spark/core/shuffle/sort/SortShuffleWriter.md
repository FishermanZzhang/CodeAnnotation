# shuffle write
spark2.x 中存在三种shuffle write 方式 1. BypassMergeSortShuffleWriter 2. SortShuffleWriter 3. UnsafeShuffleWriter 
spark3.x 中只有了SortShuffleWriter。

[SortShuffleWriter](https://github.com/apache/spark/blob/v3.0.1/core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleWriter.scala#L51)
[ExternalSorter](https://github.com/apache/spark/blob/v3.0.1/core/src/main/scala/org/apache/spark/util/collection/ExternalSorter.scala)
整体思路
* 根据是否map端做聚合, 初始化ExternalSorter。
   ```
   // map端需要聚合使用 map
   var map = new PartitionedAppendOnlyMap[K, C]
   // map端需要聚合使用 buffer
   var buffer = new PartitionedPairBuffer[K, C
   ```
* 如果超过内存，则进行溢写(spill)。 spill 会根据K(partitonId, hash) 进行排序。
* merge sort 
```
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // 初始化 sorter
    // 后续代码注解以 map端组聚合为例
    sorter = if (dep.mapSideCombine) {
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    // 把records 给到sorter。如果超过内存则进行spill
    sorter.insertAll(records)
    // merge sort
    sorter.writePartitionedMapOutput(dep.shuffleId, mapId, mapOutputWriter)
  }
```
以map端做聚合为例
```
  def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    val mergeValue = aggregator.get.mergeValue
    val createCombiner = aggregator.get.createCombiner
    var kv: Product2[K, V] = null
    val update = (hadValue: Boolean, oldValue: C) => {
      if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
    }
    while (records.hasNext) {
      addElementsRead()
      // 获取元素的kv
      kv = records.next()
      // key = (getPartition(kv._1), kv._1) 即(分区id， key的hash值) 
      map.changeValue((getPartition(kv._1), kv._1), update)
      // 调用栈
      maybeSpillCollection(usingMap = true)
    }
  }

  // maybeSpillCollection 会调用 maybeSpill
  protected def maybeSpill(collection: C, currentMemory: Long): Boolean = {
    var shouldSpill = false
    // 初始myMemoryThreshold=5m, 根据资源配置动态获取资源
    if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
      // Claim up to double our current memory from the shuffle memory pool
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
      // 建议看一下资源申请, 这里不展开了
      val granted = acquireMemory(amountToRequest)
      myMemoryThreshold += granted
      // If we were granted too little memory to grow further (either tryToAcquire returned 0,
      // or we already had more memory than myMemoryThreshold), spill the current collection
      // 系统资源充足, 则不会spill. 继续往内存中写
      // 申请到一定时刻, 系统资源不足, 则granted为0, 则会spill 
      shouldSpill = currentMemory >= myMemoryThreshold
    }
    // numElementsForceSpillThreshold 初始值为 Int.max_value
    shouldSpill = shouldSpill || _elementsRead > numElementsForceSpillThreshold
    // Actually spill
    if (shouldSpill) {
      _spillCount += 1
      logSpillage(currentMemory)
      // 进行溢写, 详细说明如下
      spill(collection)
      _elementsRead = 0
      _memoryBytesSpilled += currentMemory
      releaseMemory()
    }
    shouldSpill
  }

  // 开始溢写
  override protected[this] def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    // 这里comparator 
    // 会按照 key = (getPartition(kv._1), kv._1) 即(分区id， key的hash值) 进行排序
    val inMemoryIterator = collection.destructiveSortedWritablePartitionedIterator(comparator)
    val spillFile = spillMemoryIteratorToDisk(inMemoryIterator)
    // 把溢写的文件加入列表中spills,后续进行merge sort
    spills += spillFile
  }  
```
至此, 把所有的record 插入到了 collection. collection 可能包含内存资源和spill 文件
针对每个partition 执行 merge sort
```
// 调用链 --> 表示函数调用  
// sorter.writePartitionedMapOutput -->
// partitionedIterator (这里是按partitionId 进行merge `for ((id, elements) <- this.partitionedIterator)`)  -->
// merge 在这个可以看到spills 和 内存数据 merge -->
// mergeWithAggregation -->
// mergeSort 通过迭代器完成算法部分. 
  /**
   * Merge-sort a sequence of (K, C) iterators using a given a comparator for the keys.
   */
  private def mergeSort(iterators: Seq[Iterator[Product2[K, C]]], comparator: Comparator[K])
      : Iterator[Product2[K, C]] = {
    val bufferedIters = iterators.filter(_.hasNext).map(_.buffered)
    type Iter = BufferedIterator[Product2[K, C]]
    // Use the reverse order (compare(y,x)) because PriorityQueue dequeues the max
    // 使用优先队列进行排序
    val heap = new mutable.PriorityQueue[Iter]()(
      (x: Iter, y: Iter) => comparator.compare(y.head._1, x.head._1))
    heap.enqueue(bufferedIters: _*)  // Will contain only the iterators with hasNext = true
    new Iterator[Product2[K, C]] {
      override def hasNext: Boolean = heap.nonEmpty

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val firstBuf = heap.dequeue()
        val firstPair = firstBuf.next()
        if (firstBuf.hasNext) {
          heap.enqueue(firstBuf)
        }
        firstPair
      }
    }
  }


```

### 参考
1. https://toutiao.io/posts/eicdjo/preview