# SSTable读取流程
LevelDB的读取KV数据的操作层次为: `memtable`->`immutable memtable`->`sstable`，这里对`sstable`的读取流程做一个梳理：

![](images/SSTable/SSTable读取流程.png)