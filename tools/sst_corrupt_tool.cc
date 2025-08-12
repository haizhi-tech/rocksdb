#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>
#include <memory>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_dump_tool.h"
#include "rocksdb/status.h"
#include "table/format.h"
#include "table/sst_file_dumper.h"
#include "util/coding.h"
#include "util/gflags_compat.h"

DEFINE_string(file, "", "SST file path");
DEFINE_bool(show, false, "Show SST file structure");
DEFINE_bool(corrupt, false, "Corrupt specified block");
DEFINE_int32(block, -1, "Data block index to corrupt");
DEFINE_int64(offset, -1, "Offset within the block");
DEFINE_int32(len, 1, "N. Bytes to corrupt");
DEFINE_string(pattern, "flip", "Corruption method: flip, zero, one");

namespace ROCKSDB_NAMESPACE {

class SstCorruptTool {
 public:
  struct BlockInfo {
    uint64_t offset;
    uint64_t size;
    std::string type;
  };

 private:
  std::string filename_;
  std::vector<BlockInfo> blocks_;
  std::unique_ptr<SstFileDumper> dumper_;
  uint64_t file_size_;

 public:
  Status ParseFile(const std::string& filename) {
    filename_ = filename;

    // Get file size
    struct stat st;
    if (stat(filename_.c_str(), &st) != 0) {
      return Status::IOError("Cannot stat file");
    }
    file_size_ = st.st_size;

    // Create SstFileDumper to handle SST parsing
    Options options;
    dumper_ = std::make_unique<SstFileDumper>(options, filename,
                                              Temperature::kUnknown, 0, false,
                                              false, false, EnvOptions(), true);

    if (dumper_->getStatus() != Status::OK()) {
      return dumper_->getStatus();
    }

    // Get BlockHandles using SstFileDumper's new method
    std::vector<BlockHandle> block_handles;
    Status s = dumper_->GetDataBlockHandles(block_handles);
    if (!s.ok()) {
      return s;
    }

    // Clear any existing data blocks and convert BlockHandles to BlockInfo
    blocks_.clear();
    for (const auto& handle : block_handles) {
      BlockInfo info;
      info.offset = handle.offset();
      info.size = handle.size();
      info.type = "";

      // Only add valid blocks (non-zero size)
      if (info.size > 0) {
        blocks_.push_back(info);
      }
    }

    return Status::OK();
  }

  void ShowStructure() {
    std::cout << "SST File: " << filename_ << std::endl;
    std::cout << "File size: " << file_size_ << " bytes" << std::endl;

    // Get table properties from SstFileDumper
    std::shared_ptr<const TableProperties> table_props;
    Status s = dumper_->ReadTableProperties(&table_props);
    if (s.ok() && table_props) {
      std::cout << "Format version: " << table_props->format_version
                << std::endl;
      std::cout << "Data size: " << table_props->data_size << " bytes"
                << std::endl;
      std::cout << "Index size: " << table_props->index_size << " bytes"
                << std::endl;
      std::cout << "Number of entries: " << table_props->num_entries
                << std::endl;
      std::cout << "Number of data blocks: " << table_props->num_data_blocks
                << std::endl;
    }
    std::cout << std::endl;

    if (!blocks_.empty()) {
      std::cout << "Blocks (" << blocks_.size() << " blocks):" << std::endl;
      for (size_t i = 0; i < blocks_.size(); i++) {
        std::cout << "  [" << i << "] offset=" << blocks_[i].offset
                  << ", size=" << blocks_[i].size << " bytes" << std::endl;
      }
      std::cout << std::endl;
    } else {
      std::cout << "No blocks found" << std::endl << std::endl;
    }
  }

  Status CorruptBlock(int block_index, uint64_t offset_in_block, size_t length,
                      const std::string& pattern) {
    if (block_index < 0 || block_index >= static_cast<int>(blocks_.size())) {
      return Status::InvalidArgument("Invalid block index");
    }

    const BlockInfo& block = blocks_[block_index];

    // Check bounds
    if (offset_in_block + length > block.size) {
      return Status::InvalidArgument(
          "Corruption extends beyond block boundary");
    }

    // Calculate actual file offset
    uint64_t file_offset = block.offset + offset_in_block;

    // Open file for writing
    int fd = open(filename_.c_str(), O_RDWR);
    if (fd < 0) {
      return Status::IOError("Cannot open file for writing");
    }

    // Seek to position
    if (lseek(fd, file_offset, SEEK_SET) != static_cast<off_t>(file_offset)) {
      close(fd);
      return Status::IOError("Cannot seek to position");
    }

    // Read original bytes
    std::vector<uint8_t> buffer(length);
    if (read(fd, buffer.data(), length) != static_cast<ssize_t>(length)) {
      close(fd);
      return Status::IOError("Cannot read original bytes");
    }

    // Apply corruption pattern
    if (pattern == "flip") {
      for (size_t i = 0; i < length; i++) {
        buffer[i] ^= 0xFF;
      }
    } else if (pattern == "zero") {
      std::fill(buffer.begin(), buffer.end(), 0x00);
    } else if (pattern == "one") {
      std::fill(buffer.begin(), buffer.end(), 0xFF);
    } else {
      close(fd);
      return Status::InvalidArgument("Unknown corruption pattern");
    }

    // Write corrupted bytes back
    if (lseek(fd, file_offset, SEEK_SET) != static_cast<off_t>(file_offset)) {
      close(fd);
      return Status::IOError("Cannot seek to position for writing");
    }

    if (write(fd, buffer.data(), length) != static_cast<ssize_t>(length)) {
      close(fd);
      return Status::IOError("Cannot write corrupted bytes");
    }

    // Sync to disk
    fsync(fd);
    close(fd);

    std::cout << "Corrupted " << length << " bytes at block " << block_index
              << " offset " << offset_in_block << " (file offset "
              << file_offset << ") with pattern: " << pattern << std::endl;

    return Status::OK();
  }
};

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  gflags::SetUsageMessage(
      "SST blocks viewer and corruption tool\n"
      "\n"
      "Usage:\n"
      "  sst_corrupt_tool --helpshort\n"
      "  sst_corrupt_tool --file=<path> --show\n"
      "  sst_corrupt_tool --file=<path> --corrupt --block=<n> --offset=<n> "
      "--pattern=flip");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_file.empty()) {
    std::cerr << "Error: --file is required" << std::endl;
    return 1;
  }

  ROCKSDB_NAMESPACE::SstCorruptTool tool;
  ROCKSDB_NAMESPACE::Status s = tool.ParseFile(FLAGS_file);
  if (!s.ok()) {
    std::cerr << "Error parsing file: " << s.ToString() << std::endl;
    return 1;
  }

  if (FLAGS_show) {
    tool.ShowStructure();
  }

  if (FLAGS_corrupt) {
    if (FLAGS_block < 0 || FLAGS_offset < 0) {
      std::cerr << "Error: --block and --offset are required for corruption"
                << std::endl;
      return 1;
    }

    s = tool.CorruptBlock(FLAGS_block, FLAGS_offset, FLAGS_len, FLAGS_pattern);
    if (!s.ok()) {
      std::cerr << "Error corrupting block: " << s.ToString() << std::endl;
      return 1;
    }
  }

  if (!FLAGS_show && !FLAGS_corrupt) {
    std::cerr << "Please specify --show or --corrupt" << std::endl;
    return 1;
  }

  return 0;
}
