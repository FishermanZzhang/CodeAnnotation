从源码的角度看sql 的执行流程
* select
* insert
* optimize

## select
`select * from $table_name`

src/Server/TCPHandler.cpp
```
void TCPHandler::runImpl(){
while(true){
// executeQuery负责从sql构建pipline
state.io = executeQuery(state.query, *query_context, false, state.stage, may_have_embedded_data)
// 执行pipeline, 把执行结果发动到client
processOrdinaryQueryWithProcessors();
}
}
```
src/Interpreters/executeQuery.cpp
```
executeQuery --> executeQueryImpl
static std::tuple<ASTPtr, BlockIO> executeQueryImpl(...){
// 把sql 语句转换成 抽象语法树AST
ast = parseQuery(parser, begin, end, "", max_query_size, settings.max_parser_depth)
// 根据ast 类型获取 解释器
// 这里的interpreter 类型为InterpreterSelectWithUnionQuery(src/Interpreters/InterpreterSelectWithUnionQuery.cpp)
auto interpreter = InterpreterFactory::get(ast, context, SelectQueryOptions(stage).setInternal(internal))
// 构建queryplan 和 pipeline
res = interpreter->execute();
// 这里的res 的类型为BlockIO, 这个struct 包含了 inputstream 和 outputstream
res.finish_callback = std::move(finish_callback);
res.exception_callback = std::move(exception_callback);
}
```
这里对interpreter->execute() 展开
src/Interpreters/InterpreterSelectWithUnionQuery.cpp
```
BlockIO InterpreterSelectWithUnionQuery::execute(){
  BlockIO res;
  QueryPlan query_plan;
  // 构建执行计划（物理执行计划）
  buildQueryPlan(query_plan);
  // 构建pipeline(可执行物理计划)
  auto pipeline = query_plan.buildQueryPipeline();
  res.pipeline = std::move(*pipeline);
  res.pipeline.addInterpreterContext(context);
  return res;
}
```
展开构建执行计划的过程
```
void InterpreterSelectWithUnionQuery::buildQueryPlan(QueryPlan & query_plan){
// 这里的 解释器类型InterpreterSelectQuery
nested_interpreters.front()->buildQueryPlan(query_plan);
}
```
src/Interpreters/InterpreterSelectQuery.cpp
```
// 构建QueryPlan，包含很多step. 跟多的细节参考构造函数
buildQueryPlan --> executeImpl --> executeFetchColumns
// 通过executeFetchColumns 添加了源数据流（inputstream）
void InterpreterSelectQuery::executeFetchColumns(...){
// 忽略一些不同的细节
// 关联上了的源头， 注意此时还没有真正的读数据。
storage->read(query_plan, required_columns, metadata_snapshot,
              query_info, *context, processing_stage, max_block_size, max_streams);

// 如果是MergerTree，storage 类型为 StorageMergeTree (src/Storages/StorageMergeTree.cpp
// 如果是Distribute，storage 类型为 StorageDistributed (src/Storages/StorageDistributed.cpp)
// 其他引擎类似
// 这些引擎都继承了 统一的接口IStorage(src/Storages/IStorage.h)


}              
```
src/Storages/StorageMergeTree.cpp
```
// read(QueryPlan & query_plan, ...) --> read
void StorageMergeTree::read(...){
// 这里的reader 类型为MergeTreeDataSelectExecutor
if (auto plan = reader.read(column_names, metadata_snapshot, query_info, context, max_block_size, num_streams))
    query_plan = std::move(*plan)
}
```
src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp
```
read --> readFromParts --> spreadMarkRangesAmongStreams
// (1)确定一个合适的stream数量
// (2)创建MergeTreeReadPool
// (3)给每个stream创建一个MergeTreeThreadSelectBlockInputProcessor(src/Storages/MergeTree/MergeTreeThreadSelectBlockInputProcessor.h)，并关联到pool上
// (4)createPlanFromPipe()
QueryPlanPtr MergeTreeDataSelectExecutor::spreadMarkRangesAmongStreams(...){
// TODO: 代码分析
}
```

// 这些引擎后续再分析，先回到sql 的执行流程上
interpreter->execute() 过程经过一系列的优化、关联源数据，构造出queryplan。再经过（物理）优化构造出pipeline.

接下来就是执行pipeline.
src/Server/TCPHandler.cpp
```
void TCPHandler::processOrdinaryQueryWithProcessors(){
// 执行器
PullingAsyncPipelineExecutor executor(pipeline);
while (executor.pull(block, query_context->getSettingsRef().interactive_delay / 1000)){
  sendLogs();
  if (block){
    if (!state.io.null_format)
      sendData(block);
  }
}
}
```
src/Processors/Executors/PullingAsyncPipelineExecutor.cpp
```
bool PullingAsyncPipelineExecutor::pull(Block & block, uint64_t milliseconds){
  // Chunk is a list of columns with the same length.
  Chunk chunk;
  if (!pull(chunk, milliseconds))
      return false;
  block = lazy_format->getPort(IOutputFormat::PortKind::Main).getHeader().cloneWithColumns(chunk.detachColumns());
}

bool PullingAsyncPipelineExecutor::pull(Chunk & chunk, uint64_t milliseconds){
  if (!data){
    data = std::make_unique<Data>();
    // 这里的executor 为PipelineExecutor(src/Processors/Executors/PipelineExecutor.cpp)
    data->executor = pipeline.execute();

    auto func = [&, thread_group = CurrentThread::getGroup()]()
    {
        threadFunction(*data, thread_group, pipeline.getNumThreads());
    };

    data->thread = ThreadFromGlobalPool(std::move(func));
  }
}
```

MergeTreeThreadSelectBlockInputProcessor 继承自 MergeTreeBaseSelectProcessor

在MergeTreeDataSelectExecutor 中关联了 Processor

src/Storages/MergeTree/MergeTreeBaseSelectProcessor.cpp
```
generate --> readFromPartImpl
Chunk MergeTreeBaseSelectProcessor::readFromPartImpl(){
// range_reader  类型为
// MergeTreeRangeReader::Stream(src/Storages/MergeTree/MergeTreeRangeReader.cpp)
auto read_result = task->range_reader.read(rows_to_read, task->mark_ranges);
...

Columns ordered_columns;
ordered_columns.reserve(header_without_virtual_columns.columns());

/// Reorder columns. TODO: maybe skip for default case.
for (size_t ps = 0; ps < header_without_virtual_columns.columns(); ++ps){
  auto pos_in_sample_block = sample_block.getPositionByName(header_without_virtual_columns.getByPosition(ps).name);
  ordered_columns.emplace_back(std::move(read_result.columns[pos_in_sample_block]));
}

return Chunk(std::move(ordered_columns), read_result.num_rows)
}
```

## insert
`insert into $table_name values (...),(...)`

src/Server/TCPHandler.cpp
```
void TCPHandler::runImpl(){
while(true){
// executeQuery负责从sql构建pipline
state.io = executeQuery(state.query, *query_context, false, state.stage, may_have_embedded_data)
// 执行pipeline, 把执行结果发动到client
state.need_receive_data_for_insert = true;
processInsertQuery(connection_settings);
}
}
```
src/Interpreters/executeQuery.cpp
```
// 这部分和select过程类似
// interpreter的类型为InterpreterInsertQuery
```
这里对interpreter->execute() 展开
src/Interpreters/InterpreterInsertQuery.cpp
```
BlockIO InterpreterInsertQuery::execute(){
  // ...
  out = std::make_shared<PushingToViewsBlockOutputStream>(table, metadata_snapshot, context, query_ptr, no_destination);
}
```
src/DataStreams/PushingToViewsBlockOutputStream.cpp
```
// PushingToViewsBlockOutputStream的构造函数
PushingToViewsBlockOutputStream::PushingToViewsBlockOutputStream(...){
  // output 类型为 BlockOutputStreamPtr
  // storage 类型为 StorageMergeTree
  output = storage->write(query_ptr, storage->getInMemoryMetadataPtr(), context);
}
```
src/Storages/StorageMergeTree.cpp
```
BlockOutputStreamPtr StorageMergeTree::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & context){
    const auto & settings = context.getSettingsRef();
    return std::make_shared<MergeTreeBlockOutputStream>(
        *this, metadata_snapshot, settings.max_partitions_per_insert_block, context.getSettingsRef().optimize_on_insert);

}
```
至此，关联上了OutputStream。

经过一系列的调用，进行write操作
栈信息为：
```
DB::MergeTreeBlockOutputStream::write(DB::MergeTreeBlockOutputStream * const this, const DB::Block & block) (src\Storages\MergeTree\MergeTreeBlockOutputStream.cpp:26)
DB::PushingToViewsBlockOutputStream::write(DB::PushingToViewsBlockOutputStream * const this, const DB::Block & block) (src\DataStreams\PushingToViewsBlockOutputStream.cpp:167)
DB::AddingDefaultBlockOutputStream::write(DB::AddingDefaultBlockOutputStream * const this, const DB::Block & block) (src\DataStreams\AddingDefaultBlockOutputStream.cpp:10)
DB::SquashingBlockOutputStream::finalize(DB::SquashingBlockOutputStream * const this) (src\DataStreams\SquashingBlockOutputStream.cpp:30)
DB::SquashingBlockOutputStream::writeSuffix(DB::SquashingBlockOutputStream * const this) (src\DataStreams\SquashingBlockOutputStream.cpp:50)
DB::CountingBlockOutputStream::writeSuffix(DB::CountingBlockOutputStream * const this) (src\DataStreams\CountingBlockOutputStream.h:37)
DB::TCPHandler::processInsertQuery(DB::TCPHandler * const this, const DB::Settings & connection_settings) (src\Server\TCPHandler.cpp:520)
DB::TCPHandler::runImpl(DB::TCPHandler * const this) (src\Server\TCPHandler.cpp:268)
DB::TCPHandler::run(DB::TCPHandler * const this) (src\Server\TCPHandler.cpp:1417)
```
写操作
src/Storages/MergeTree/MergeTreeBlockOutputStream.cpp
```
void MergeTreeBlockOutputStream::write(const Block & block){
  // 把block 分成多个 partition
  auto part_blocks = storage.writer.splitBlockIntoParts(block, max_parts_per_block, metadata_snapshot);
  for (auto & current_block : part_blocks){
    // storage.writer 类型为MergeTreeDataWriter (src/Storages/MergeTree/MergeTreeDataWriter.cpp)
    MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block, metadata_snapshot, optimize_on_insert);
    // 把临时文件rename成正式文件
    storage.renameTempPartAndAdd(part, &storage.increment);
    PartLog::addNewPart(storage.global_context, part, watch.elapsed());
    storage.background_executor.triggerTask();
  }
```
src/Storages/MergeTree/MergeTreeDataWriter.cpp
```
MergeTreeData::MutableDataPartPtr MergeTreeDataWriter::writeTempPart(BlockWithPartition & block_with_partition, const StorageMetadataPtr & metadata_snapshot, bool optimize_on_insert) {

    // 按照 primary key 排序 
    Names sort_columns = metadata_snapshot->getSortingKeyColumns();
    SortDescription sort_description;
    size_t sort_columns_size = sort_columns.size();
    sort_description.reserve(sort_columns_size);

    for (size_t i = 0; i < sort_columns_size; ++i)
        sort_description.emplace_back(block.getPositionByName(sort_columns[i]), 1, 1);

    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterBlocks);

    /// Sort
    IColumn::Permutation * perm_ptr = nullptr;
    IColumn::Permutation perm;
    if (!sort_description.empty())
    {
        if (!isAlreadySorted(block, sort_description))
        {
            // 对block排序， 返回排序的索引。（block的顺序并没有发生变化）
            stableGetPermutation(block, sort_description, perm);
            perm_ptr = &perm;
        }
        else
            ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterBlocksAlreadySorted);
    }

    // 根据MergeTreeDataPartType类型
    // 这里new_data_part 类型为 MergeTreeDataPartCompact
    auto new_data_part = data.createPart(
        part_name,
        data.choosePartType(expected_size, block.rows()),
        new_part_info,
        createVolumeFromReservation(reservation, volume),
        TMP_PREFIX + part_name);
    // ...

    MergedBlockOutputStream out(new_data_part, metadata_snapshot, columns, index_factory.getMany(metadata_snapshot->getSecondaryIndices()), compression_codec);
    bool sync_on_insert = data.getSettings()->fsync_after_insert;
    // 写操作
    out.writePrefix();
    out.writeWithPermutation(block, perm_ptr);  
    out.writeSuffixAndFinalizePart(new_data_part, sync_on_insert);
}
```

经过调用栈（调用栈信息如下），从`MergeTreeDataWriter::writeTempPart` 到`MergeTreeDataPartWriterCompact::writeDataBlock`
```
DB::MergeTreeDataPartWriterCompact::writeDataBlock(DB::MergeTreeDataPartWriterCompact * const this, const DB::Block & block, const DB::Granules & granules) (src\Storages\MergeTree\MergeTreeDataPartWriterCompact.cpp:172)
DB::MergeTreeDataPartWriterCompact::writeDataBlockPrimaryIndexAndSkipIndices(DB::MergeTreeDataPartWriterCompact * const this, const DB::Block & block, const DB::Granules & granules_to_write) (src\Storages\MergeTree\MergeTreeDataPartWriterCompact.cpp:158)
DB::MergeTreeDataPartWriterCompact::finishDataSerialization(DB::MergeTreeDataPartWriterCompact * const this, DB::IMergeTreeDataPart::Checksums & checksums, bool sync) (src\Storages\MergeTree\MergeTreeDataPartWriterCompact.cpp:228)
DB::MergeTreeDataPartWriterCompact::finish(DB::MergeTreeDataPartWriterCompact * const this, DB::IMergeTreeDataPart::Checksums & checksums, bool sync) (src\Storages\MergeTree\MergeTreeDataPartWriterCompact.cpp:356)
DB::MergedBlockOutputStream::writeSuffixAndFinalizePart(DB::MergedBlockOutputStream * const this, DB::MergeTreeData::MutableDataPartPtr & new_part, bool sync, const DB::NamesAndTypesList * total_columns_list, DB::IMergeTreeDataPart::Checksums * additional_column_checksums) (\home\zhangzhen\ClickHouse\src\Storages\MergeTree\MergedBlockOutputStream.cpp:73)
DB::MergeTreeDataWriter::writeTempPart(DB::MergeTreeDataWriter * const this, DB::BlockWithPartition & block_with_partition, const DB::StorageMetadataPtr & metadata_snapshot, bool optimize_on_insert) (\home\zhangzhen\ClickHouse\src\Storages\MergeTree\MergeTreeDataWriter.cpp:406)
```


## optimize

# 参考文献
* http://sineyuan.github.io/post/clickhouse-source-guide/
* https://zhuanlan.zhihu.com/p/148762641