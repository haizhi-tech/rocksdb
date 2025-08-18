#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>

#include <iostream>
#include <memory>

int main(int argc, char** argv) {
  std::string db_path = "/tmp/testdb";
  size_t block_size = 1024;  // default 1KB

  if (argc > 1) {
    db_path = argv[1];
  }
  if (argc > 2) {
    block_size = std::stoul(argv[2]);
  }

  rocksdb::Options options;
  options.create_if_missing = true;

  // Set block size for testing
  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_size = block_size;
  options.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(table_options));

  // Remove existing DB if present
  rocksdb::DestroyDB(db_path, options);

  rocksdb::DB* db_ptr;
  rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db_ptr);
  if (!status.ok()) {
    std::cerr << "Failed to open DB: " << status.ToString() << std::endl;
    return 1;
  }

  std::unique_ptr<rocksdb::DB> db;
  db.reset(db_ptr);

  // Write some data
  for (int i = 0; i < 1000; i++) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    db->Put(rocksdb::WriteOptions(), key, value);
  }

  // Force flush to create SST files
  db->Flush(rocksdb::FlushOptions());

  std::cout << "DB created at " << db_path << std::endl;
  std::cout << "Block size: " << block_size << " bytes" << std::endl;

  return 0;
}
