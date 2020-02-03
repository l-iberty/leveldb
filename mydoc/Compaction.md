# Compaction
`DBImpl::BackgroundCompaction()`负责Minor Compaction和Major Compaction.

## Major Compaction
`DBImpl::DoCompactionWork()`对之间选好的SSTable文件进行major compaction, 把`level`层中的一些文件合并到`level+1`层中与之重叠的文件里面. compaction生成的新文件被记录在`level+1`层, 记录工作由`DBImpl::InstallCompactionResults()`完成, 代码片段:
```cpp
Status DBImpl::InstallCompactionResults(CompactionState* compact) {
	...
	 compact->compaction->edit()->AddFile(level + 1,
	...
}
```
之后`level`层和`level+1`层里参与compaction的文件会被删除. LSM-Tree的写放大就在于, major compaction时`level`层文件与`level+1`层文件合并时导致`level+1`层里参与compaction的文件被重写.

leveldb在使用`MergingIterator`实现SSTable文件的合并排序, 也就是对所有参与compaction的文件构建了一个"合并迭代器", 见`DBImpl::DoCompactionWork()`里的一段代码:
```cpp
  Iterator* input = versions_->MakeInputIterator(compact->compaction);
```