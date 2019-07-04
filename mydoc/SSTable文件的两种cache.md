# SSTable文件的两种cache
## TableCache
代码: `db/table_cache.cc`

索引型缓存，缓存在cache中的key是sstable文件的`file_number`， value是`TableAndFile`对象. `TableAndFile`封装了一个`Table`对象和`RandomAccessFile`对象，`Table`对象保存了sstable文件中的索引模块(Meta Index Block和Index Block)以及FilterBlock，没有缓存各个DataBlock; `RandomAccessFile`对象指向sstable文件，根据`Table`对象的索引信息按需从中读出`DataBlock`.

## BlockCache
代码: `table/table.cc`

并没有一个名为`BlockCache`的类，BlockCache是一个概念，它只是`Options`中一个名为`block_cache`的`Cache`对象. BlockCache是数据型缓存，缓存的是DataBlock的具体数据，详见`Table::BlockReader()`.