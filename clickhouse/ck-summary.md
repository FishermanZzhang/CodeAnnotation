## 复制表连续insert相同数据问题
问题描述：第一插入shard时， 可以成功。 第二次插入（该shard和第一次的shard互为副本），则插入不成功

src/Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.cpp
```
// 1. 获取block_id
block_id = part->getZeroLevelPartBlockID(); // 实现 src/Storages/MergeTree/IMergeTreeDataPart.cpp

// 2. 判断节点是否存在
bool deduplicate_block = !block_id.empty();
String block_id_path = deduplicate_block ? storage.zookeeper_path + "/blocks/" + block_id : "";
auto block_number_lock = storage.allocateBlockNumber(part->info.partition_id, zookeeper, block_id_path);
```