//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <iostream>
#include <string>
#include <vector>

#include "rocksdb/db.h"

using namespace ROCKSDB_NAMESPACE;

void PrintUsage() {
  std::cerr << "Usage: print_all_keys <db_path> [--hex] "
               "[--cf=<column_family_name>] [--skip-corrupted-data-blocks]"
            << std::endl;
  std::cerr << "  --hex: Print keys and values in hexadecimal format"
            << std::endl;
  std::cerr << "  --cf=<name>: Specify column family name (default: all column "
               "families)"
            << std::endl;
  std::cerr << "  --skip-corrupted-data-blocks: Skip corrupted data blocks "
               "during iteration (for data recovery)"
            << std::endl;
  std::cerr << "  If no --cf specified, prints from all column families"
            << std::endl;
}

std::string ToHex(const std::string& str) {
  std::string result;
  for (unsigned char c : str) {
    char buf[3];
    snprintf(buf, sizeof(buf), "%02x", c);
    result += buf;
  }
  return result;
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    PrintUsage();
    return 1;
  }

  std::string db_path = argv[1];
  bool use_hex = false;
  std::string target_cf = "";
  bool specific_cf = false;
  bool skip_corrupted_data_blocks = false;

  // Parse command line arguments
  for (int i = 2; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--hex") {
      use_hex = true;
    } else if (arg.substr(0, 5) == "--cf=") {
      target_cf = arg.substr(5);
      specific_cf = true;
    } else if (arg == "--skip-corrupted-data-blocks") {
      skip_corrupted_data_blocks = true;
    } else {
      std::cerr << "Unknown option: " << argv[i] << std::endl;
      PrintUsage();
      return 1;
    }
  }

  // First, list all column families
  std::vector<std::string> cf_names;
  Status s = DB::ListColumnFamilies(Options(), db_path, &cf_names);
  if (!s.ok()) {
    std::cerr << "Failed to list column families: " << s.ToString()
              << std::endl;
    return 1;
  }

  std::cout << "Available column families: ";
  for (size_t i = 0; i < cf_names.size(); i++) {
    std::cout << cf_names[i];
    if (i < cf_names.size() - 1) std::cout << ", ";
  }
  std::cout << std::endl << std::endl;

  // If specific CF requested, check if it exists
  if (specific_cf) {
    bool found = false;
    for (const auto& name : cf_names) {
      if (name == target_cf) {
        found = true;
        break;
      }
    }
    if (!found) {
      std::cerr << "Column family '" << target_cf << "' not found!"
                << std::endl;
      return 1;
    }
  }

  // Prepare column family descriptors
  std::vector<ColumnFamilyDescriptor> column_families;
  for (const auto& name : cf_names) {
    column_families.emplace_back(name, ColumnFamilyOptions());
  }

  // Open database with all column families
  DB* db;
  std::vector<ColumnFamilyHandle*> handles;
  Options db_options;

  s = DB::Open(db_options, db_path, column_families, &handles, &db);
  if (!s.ok()) {
    std::cerr << "Failed to open database: " << s.ToString() << std::endl;
    return 1;
  }

  uint64_t total_count = 0;

  // Iterate through column families
  for (size_t i = 0; i < cf_names.size(); i++) {
    // Skip if specific CF requested and this isn't it
    if (specific_cf && cf_names[i] != target_cf) {
      continue;
    }

    std::cout << "=== Column Family: " << cf_names[i] << " ===" << std::endl;

    // Create iterator for this column family
    ReadOptions read_options;
    read_options.skip_corrupted_data_blocks = skip_corrupted_data_blocks;
    Iterator* it = db->NewIterator(read_options, handles[i]);

    uint64_t cf_count = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      std::string key = it->key().ToString();
      std::string value = it->value().ToString();

      if (use_hex) {
        std::cout << "Key: " << ToHex(key) << ", Value: " << ToHex(value)
                  << std::endl;
      } else {
        std::cout << "Key: " << key << ", Value: " << value << std::endl;
      }
      cf_count++;
    }

    // Check for any errors that happened during the iteration
    s = it->status();
    if (!s.ok()) {
      if (skip_corrupted_data_blocks) {
        std::cerr << "Warning: Iteration completed with error in CF '"
                  << cf_names[i] << "': " << s.ToString() << std::endl;
      } else {
        std::cerr << "Error during iteration in CF '" << cf_names[i]
                  << "': " << s.ToString() << std::endl;
        delete it;
        // Clean up handles
        for (auto* handle : handles) {
          delete handle;
        }
        delete db;
        return 1;
      }
    }

    std::cout << "Keys in CF '" << cf_names[i] << "': " << cf_count << std::endl
              << std::endl;
    total_count += cf_count;

    delete it;
  }

  std::cout << "Total keys printed: " << total_count << std::endl;

  // Clean up handles
  for (auto* handle : handles) {
    delete handle;
  }
  delete db;
  return 0;
}
