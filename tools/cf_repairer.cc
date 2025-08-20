// To statically build cf_repairer:
//
// mkdir build; cd build
// cmake .. -DCMAKE_EXPORT_COMPILE_COMMANDS=1 -DROCKSDB_BUILD_SHARED=OFF
// -DBUILD_SHARED_LIBS=OFF -DWITH_GFLAGS=1 make cf_repairer -j 16

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "file/filename.h"
#include "file/line_file_reader.h"
#include "port/port_posix.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/version.h"
#include "table/sst_file_dumper.h"
#include "util/channel.h"
#include "util/gflags_compat.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"

#define REPAIRER_LOG(LGR, FMT, ...) \
  rocksdb::Log(rocksdb::InfoLogLevel::INFO_LEVEL, LGR, FMT, ##__VA_ARGS__)

DEFINE_string(db_path, "", "The path of db to operate with");
DEFINE_string(
    cf_names, "",
    "The names of cf to operate with, multiple cf shoule be comma-separated");
DEFINE_string(command, "", "The command to run");
DEFINE_uint32(workers, 4, "The number of worker to run repair process");
DEFINE_string(corrupt_sst_path, "_corrupted_sst_list.txt",
              "The file path to store corruption sst file names");
DEFINE_bool(verbose, false, "Whether if print more informations");
DEFINE_bool(
    wal_recovery_skip_corrupted, false,
    "Whether if set wal_recovery_mode to "
    "WALRecoveryMode::kSkipAnyCorruptedRecords, in case of wal corruption");

class CfRepairer {
 public:
  CfRepairer();
  void Help();
  void Run(int argc, char** argv);

 private:
  void InitialTargetCf();
  void OpenDB(bool read_only);
  void CloseDB();
  void RunSstCheck();

  void RunSstCheckThreads(std::string, std::vector<std::vector<std::string>>,
                          rocksdb::channel<std::string>*);
  void JoinSstCheckThreads(rocksdb::channel<std::string>*);
  void ReceiveCheckResults(std::string, rocksdb::channel<std::string>*);
  rocksdb::Status CheckSst(const std::string&);

  void StoreCheckResults();

  bool ParseLine(const std::string&, std::string*, std::vector<std::string>*);
  void ReadCheckResults();
  void ShowCorruptSsts();

  rocksdb::DB* db_;
  rocksdb::ConfigOptions config_options_;
  rocksdb::Options options_;
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families_;
  std::vector<rocksdb::ColumnFamilyHandle*> cf_handles_;

  std::vector<std::string> target_cf_names_;

  std::string db_path_;
  std::string cp_path_suffix_;

  std::shared_ptr<rocksdb::Logger> logger_;

  std::vector<std::unique_ptr<rocksdb::port::Thread>> threads_;
  std::unordered_map<std::string, std::vector<std::string>> corruption_ssts_;
};

const char* USAGE =
    "USAGE: \n"
    "  cf_repairer -db_path <DBPATH> -cf_name <CFNAME> -command <COMMAND> "
    "[OPTIONS]...\n";

const char* STAGE_0 = "LoadOptions";
const char* STAGE_1 = "OpenDB";
const char* STAGE_2 = "CheckSst";
const char* STAGE_3 = "StoreCheckResult";
const char* STAGE_4 = "ReadCheckResult";

void ChunkSstFiles(const std::vector<std::string>& files,
                   std::vector<std::vector<std::string>>* results) {
  size_t min_chunk_size = 3;
  size_t chunk_size = files.size() / FLAGS_workers + 1;
  chunk_size = std::max(min_chunk_size, chunk_size);
  if (files.empty()) {
    fprintf(stdout, "[%s] sst-files empty! \n", STAGE_2);
    exit(-1);
  }

  fprintf(stdout, "[%s] sst-files count: %ld, workers: %d, chunk-size: %ld \n",
          STAGE_2, files.size(), FLAGS_workers, chunk_size);
  auto it = files.cbegin();

  while (it != files.cend()) {
    size_t step_size =
        size_t(files.cend() - it) > chunk_size ? chunk_size : files.cend() - it;
    auto it_part_end = it + step_size;
    results->emplace_back(it, it_part_end);
    it = it_part_end;
  }
}

CfRepairer::CfRepairer()
    : db_(nullptr), db_path_(FLAGS_db_path), cp_path_suffix_("_checkpoint") {
  logger_.reset(new rocksdb::StderrLogger());
}

void CfRepairer::InitialTargetCf() {
  if (db_path_.empty()) {
    fprintf(stdout, "[%s] dbpath not specified!\n", STAGE_0);
    Help();
    exit(-1);
  }

  target_cf_names_ = rocksdb::StringSplit(std::string(FLAGS_cf_names), ',');
  if (target_cf_names_.empty()) {
    fprintf(stdout, "[%s] cf names should be provided!\n", STAGE_0);
    Help();
    exit(-1);
  }

  fprintf(stdout, "[%s] db-path: %s\n", STAGE_0, db_path_.c_str());
  fprintf(stdout, "[%s] try to load options from db-path ... \n", STAGE_0);

  // load options
  rocksdb::Status s = rocksdb::LoadLatestOptions(config_options_, db_path_,
                                                 &options_, &column_families_);
  if (!s.ok()) {
    fprintf(stdout, "[%s] failed to load lastest option: %s \n", STAGE_0,
            s.ToString().c_str());
    exit(-1);
  }

  if (options_.env == rocksdb::Env::Default()) {
    options_.env = config_options_.env;
  }

  // override based options
  options_.create_if_missing = false;

  if (column_families_.empty()) {
    fprintf(stdout,
            "[%s] not found column families from options, try to load from "
            "manifests\n",
            STAGE_0);

    // try to find cf by manifest
    std::vector<std::string> cf_list;
    s = rocksdb::DB::ListColumnFamilies(options_, db_path_, &cf_list);
    if (s.ok()) {
      for (auto cf_name : cf_list) {
        column_families_.emplace_back(cf_name, options_);
      }
    }
  }

  fprintf(stdout, "[%s] load options done, found %ld column families\n",
          STAGE_0, column_families_.size());

  std::unordered_set<std::string> found_cf_names;
  REPAIRER_LOG(logger_, "found column families: ");
  for (auto cf : column_families_) {
    found_cf_names.emplace(cf.name.c_str());
    REPAIRER_LOG(logger_, "  - %s", cf.name.c_str());
  }

  bool not_found_cf = false;
  for (auto tgt_cf : target_cf_names_) {
    if (found_cf_names.find(tgt_cf) == found_cf_names.end()) {
      REPAIRER_LOG(logger_, "FATAL: target cf = %s not found!", tgt_cf.c_str());
      not_found_cf = true;
    }
  }

  if (not_found_cf) {
    fprintf(stdout, "[%s] failed, because some target cf not found!\n",
            STAGE_0);
    exit(-1);
  }
}

void CfRepairer::Help() { fprintf(stdout, "%s\n", USAGE); }

void CfRepairer::OpenDB(bool read_only) {
  rocksdb::Status s;
  REPAIRER_LOG(logger_, "[%s] Try to OpenDB: %s, readonly: %d ...\n", STAGE_1,
               db_path_.c_str(), read_only);
  if (FLAGS_wal_recovery_skip_corrupted) {
    fprintf(stdout, "[%s] would use kSkipAnyCorruptedRecords to OpenDB",
            STAGE_1);
    options_.wal_recovery_mode =
        rocksdb::WALRecoveryMode::kSkipAnyCorruptedRecords;
  }
  if (read_only) {
    s = rocksdb::DB::OpenForReadOnly(options_, db_path_, column_families_,
                                     &cf_handles_, &db_);
  } else {
    s = rocksdb::DB::Open(options_, db_path_, column_families_, &cf_handles_,
                          &db_);
  }
  if (!s.ok()) {
    fprintf(stdout, "[%s] OpenDB failed: %s\n", STAGE_1, s.ToString().c_str());
    exit(-1);
  }
  REPAIRER_LOG(logger_, "[%s] OpenDB done.\n", STAGE_1);
}

void CfRepairer::CloseDB() {
  if (db_ != nullptr) {
    for (auto cfh : cf_handles_) {
      if (cfh != nullptr) {
        db_->DestroyColumnFamilyHandle(cfh);
      }
    }
    rocksdb::Status s = db_->Close();
    s.PermitUncheckedError();
    delete db_;
    db_ = nullptr;
  }
}

void CfRepairer::Run(int argc, char** argv) {
  std::string comm(FLAGS_command);

  if (comm == "cf_sst_check") {
    InitialTargetCf();
    OpenDB(true);
    RunSstCheck();
  } else if (comm == "cf_show_sst_check_result") {
    ReadCheckResults();
  } else if (comm == "cf_sst_archive") {
  } else if (comm == "cf_restore_health_sst") {
  } else {
    fprintf(stdout,
            " Unknown command: %s, available:\n"
            "  cf_sst_check, cf_show_sst_check_result, "
            "cf_restore_health_sst, cf_sst_archive\n",
            comm.c_str());
  }

  CloseDB();
}

rocksdb::Status CfRepairer::CheckSst(const std::string& file_path) {
  bool verify_checksum = true;
  size_t readahead_size = 2 * 1024 * 1024;
  bool output_hex = false;
  bool decode_blob_index = false;
  bool silent = true;
  bool print_kv = false;
  uint64_t read_num = std::numeric_limits<uint64_t>::max();  // no limit readnum
  std::string from_key;
  std::string to_key;

  rocksdb::Options opts;
  rocksdb::SstFileDumper dumper(opts, file_path, rocksdb::Temperature::kUnknown,
                                readahead_size, verify_checksum, output_hex,
                                decode_blob_index, rocksdb::EnvOptions(),
                                silent);

  rocksdb::Status s;
  s = dumper.VerifyChecksum();

  if (FLAGS_verbose) {
    REPAIRER_LOG(logger_, "[%s] sst file %s: %s", STAGE_2, file_path.c_str(),
                 s.ToString().c_str());
  }

  // we don't need to actually read every kv out?
  //
  // if (s.ok()) {
  //    s = dumper.ReadSequential(print_kv, read_num, false/*has-from*/,
  //    from_key, false /*has-to*/, to_key);
  // }
  return s;
}

void CfRepairer::RunSstCheckThreads(
    std::string cf_name,
    std::vector<std::vector<std::string>> chunked_sst_lists,
    rocksdb::channel<std::string>* ck_chan) {
  size_t thread = 0;
  for (auto chunk_ssts : chunked_sst_lists) {
    auto t = new rocksdb::port::Thread(
        [this, ck_chan](std::vector<std::string> sst_files, std::string cf,
                        size_t thread_id) {
          size_t total_files = sst_files.size();
          size_t bad_files = 0;
          for (auto file : sst_files) {
            std::string file_path(std::string(this->db_path_) + file);
            rocksdb::Status s = this->CheckSst(file_path);
            if (!s.ok()) {
              REPAIRER_LOG(this->logger_,
                           "[%s] [cf = %s] [thread = %ld] sst %s is corrupted, "
                           "detail: %s \n",
                           STAGE_2, cf.c_str(), thread_id, file.c_str(),
                           s.ToString().c_str());

              ck_chan->write(std::move(file));
              bad_files++;
            }
          }
          REPAIRER_LOG(this->logger_,
                       "[%s] [cf = %s] [thread = %ld] worker finished, "
                       "summary: %ld files checked, %ld files corrupted",
                       STAGE_2, cf.c_str(), thread_id, total_files, bad_files);
        },
        chunk_ssts, cf_name, thread);
    threads_.emplace_back(t);
    thread++;
  }
}

void CfRepairer::JoinSstCheckThreads(rocksdb::channel<std::string>* ck_chan) {
  for (auto t = threads_.begin(); t != threads_.end(); ++t) {
    if (t->get() != nullptr) {
      t->get()->join();
      t->reset();
    }
  }
  threads_.clear();
  ck_chan->sendEof();
}

void CfRepairer::ReceiveCheckResults(std::string cf_name,
                                     rocksdb::channel<std::string>* ck_chan) {
  auto& op = corruption_ssts_[cf_name];
  while (true) {
    std::string file;
    bool s = ck_chan->read(file);
    if (!s) {
      break;
    }
    op.emplace_back(std::move(file));
  }
  fprintf(stdout,
          "[%s] total corruption ssts: %ld, has checked column families: "
          "%ld \n",
          STAGE_2, corruption_ssts_[cf_name].size(), corruption_ssts_.size());
}

void CfRepairer::RunSstCheck() {
  std::unordered_map<std::string, rocksdb::ColumnFamilyHandle*> cf_handles_map;
  for (auto cfh : cf_handles_) {
    cf_handles_map.emplace(std::string(cfh->GetName()), cfh);
  }

  for (const auto& cf : target_cf_names_) {
    fprintf(stdout, "[%s] [cf = %s] start to check sst files ... \n", STAGE_2,
            cf.c_str());

    auto cfh_it = cf_handles_map.find(cf);
    if (cfh_it == cf_handles_map.end()) {
      REPAIRER_LOG(logger_,
                   "[%s] [cf = %s] skip: not found this column family!",
                   STAGE_2, cf.c_str());
      continue;
    }
    rocksdb::ColumnFamilyHandle* cf_handle = cfh_it->second;
    rocksdb::ColumnFamilyMetaData cf_metadata;
    db_->GetColumnFamilyMetaData(cf_handle, &cf_metadata);

    std::vector<std::string> cf_sst_files;
    for (const auto& lvl_md : cf_metadata.levels) {
      for (const auto& f_md : lvl_md.files) {
        uint64_t number;
        rocksdb::FileType type;
        auto s = rocksdb::ParseFileName(f_md.name, &number, &type);
        if (!s) {
          REPAIRER_LOG(logger_, "[%s] [cf = %s] corruption: bad file name %s!",
                       STAGE_2, cf.c_str(), f_md.name.c_str());
          return;
        }
        if (type == rocksdb::kTableFile) {
          cf_sst_files.emplace_back(f_md.name);
        }
      }
    }
    fprintf(stdout, "[%s] [cf = %s] has found %ld sst files to check.\n",
            STAGE_2, cf.c_str(), cf_sst_files.size());

    if (cf_sst_files.empty()) {
      fprintf(stdout, "[%s] [cf = %s] has no ssts, skipped ... \n", STAGE_2,
              cf.c_str());
      continue;
    }

    // chunk sst
    std::vector<std::vector<std::string>> chunked_sst_lists;
    ChunkSstFiles(cf_sst_files, &chunked_sst_lists);

    int ck = 0;
    size_t chunk_total = 0;
    fprintf(stdout, "[%s] [cf = %s] has %ld chunks, workers: %d. \n", STAGE_2,
            cf.c_str(), chunked_sst_lists.size(), FLAGS_workers);

    for (const auto& item : chunked_sst_lists) {
      fprintf(stdout, "[%s] [cf = %s] chunk %d size: %ld \n", STAGE_2,
              cf.c_str(), ck, item.size());
      chunk_total += item.size();
      ck++;
    }

    if (chunked_sst_lists.size() > FLAGS_workers) {
      fprintf(stdout,
              "[%s] [cf = %s] [BUG] Bad chunk results: chunks more than "
              "workers. \n",
              STAGE_2, cf.c_str());
      exit(-1);
    }

    if (chunk_total != cf_sst_files.size()) {
      fprintf(stdout,
              "[%s] [cf = %s] [BUG] Bad chunk results: chunked sst lists not "
              "equal to original. \n",
              STAGE_2, cf.c_str());
      exit(-1);
    }

    // check sst
    fprintf(stdout, "[%s] [cf = %s] + start sst check workers ... \n", STAGE_2,
            cf.c_str());
    rocksdb::channel<std::string> ck_chan;
    RunSstCheckThreads(cf, std::move(chunked_sst_lists), &ck_chan);
    fprintf(stdout,
            "[%s] [cf = %s] + wait sst check workers to "
            "finish ... \n",
            STAGE_2, cf.c_str());
    JoinSstCheckThreads(&ck_chan);
    fprintf(stdout, "[%s] [cf = %s] + all sst check workers finished.\n",
            STAGE_2, cf.c_str());
    ReceiveCheckResults(cf, &ck_chan);
    fprintf(stdout,
            "[%s] [cf = %s] + all sst check workers results kept finished.\n",
            STAGE_2, cf.c_str());
  }

  StoreCheckResults();
  fprintf(stdout, "[%s] + store checking results into %s.\n", STAGE_2,
          FLAGS_corrupt_sst_path.c_str());
}

void CfRepairer::StoreCheckResults() {
  size_t total = 0;
  for (const auto& i : corruption_ssts_) {
    total += i.second.size();
  }

  std::string output_path(FLAGS_corrupt_sst_path);
  const rocksdb::EnvOptions soptions;
  std::unique_ptr<rocksdb::WritableFile> output_file;
  rocksdb::Status s =
      options_.env->NewWritableFile(output_path, &output_file, soptions);
  if (!s.ok()) {
    fprintf(stdout, "[%s] Open output file(%s) failed: %s \n", STAGE_3,
            output_path.c_str(), s.ToString().c_str());
    return;
  }
  fprintf(stdout, "[%s] Output %ld sst names to file: %s \n", STAGE_3, total,
          output_path.c_str());

  for (const auto& cf : corruption_ssts_) {
    output_file->Append("CF:");
    output_file->Append(cf.first);
    output_file->Append(";");
    for (const auto& item : cf.second) {
      output_file->Append(item);
      output_file->Append(",");
    }
    output_file->Append("\n");
  }

  if (total == 0) {
    fprintf(stdout, "[%s] No corrupted sst found! \n", STAGE_3);
  }
  output_file->Fsync();
  output_file->Close();
}

bool CfRepairer::ParseLine(const std::string& line, std::string* cf,
                           std::vector<std::string>* ssts) {
  cf->clear();
  ssts->clear();
  rocksdb::Slice s(line);

  if (line.empty()) {
    fprintf(stdout, "[%s] empty line, skip! \n", STAGE_4);
    return false;
  }
  if (!s.starts_with("CF:")) {
    fprintf(stdout, "[%s] parse failed, origianl text: %s \n", STAGE_4,
            line.c_str());
    return false;
  }
  std::string raw = line.substr(3);
  std::vector<std::string> cf_and_ssts = rocksdb::StringSplit(raw, ';');
  if (cf_and_ssts.size() > 2) {
    fprintf(stdout, "[%s] parse failed, origianl text: %s \n", STAGE_4,
            line.c_str());
    return false;
  }

  if (cf_and_ssts.size() == 1) {
    cf_and_ssts.push_back("");
  }

  cf->assign(cf_and_ssts[0]);
  if (!cf_and_ssts[1].empty()) {
    std::vector<std::string> sst_names =
        rocksdb::StringSplit(cf_and_ssts[1], ',');
    for (const auto& i : sst_names) {
      if (!i.empty()) {
        ssts->emplace_back(i);
      }
    }
  }
  fprintf(stdout, "[%s] parse success, cf = %s, sst counts = %ld \n", STAGE_4,
          cf->c_str(), ssts->size());
  return true;
}

void CfRepairer::ReadCheckResults() {
  const rocksdb::EnvOptions soptions;
  std::shared_ptr<rocksdb::FileSystem> fs = options_.env->GetFileSystem();
  std::unique_ptr<rocksdb::LineFileReader> input_file;

  std::string input_path(FLAGS_corrupt_sst_path);
  rocksdb::Status s = rocksdb::LineFileReader::Create(
      fs, input_path, rocksdb::FileOptions(), &input_file, nullptr, nullptr);
  if (!s.ok()) {
    fprintf(stdout, "[%s] Open input file(%s) failed: %s \n", STAGE_4,
            input_path.c_str(), s.ToString().c_str());
    return;
  }

  int lines = 0;
  std::string buf;
  std::string cf;
  std::vector<std::string> ssts;

  while (input_file->ReadLine(&buf, rocksdb::Env::IO_TOTAL)) {
    if (ParseLine(buf, &cf, &ssts)) {
      corruption_ssts_.insert(std::make_pair(cf, ssts));
    }
    lines++;
  }
  if (!buf.empty()) {
    if (ParseLine(buf, &cf, &ssts)) {
      corruption_ssts_.insert(std::make_pair(cf, ssts));
    }
  }

  fprintf(stdout,
          "[%s] read corrupted sst list from %s success, %d lines parsed\n",
          STAGE_4, input_path.c_str(), lines);
  ShowCorruptSsts();
}

void CfRepairer::ShowCorruptSsts() {
  REPAIRER_LOG(logger_, "======Corrupted Ssts=======");
  for (const auto& cf : corruption_ssts_) {
    REPAIRER_LOG(logger_, "- CF: %s", cf.first.c_str());
    std::string sst_list;
    for (const auto& sst : cf.second) {
      sst_list.append(sst);
      sst_list.append(",");
    }
    REPAIRER_LOG(logger_, "   %s",
                 sst_list.empty() ? "<EMPTY>" : sst_list.c_str());
  }
  REPAIRER_LOG(logger_, "===========================");
}

int main(int argc, char** argv) {
  gflags::SetVersionString(rocksdb::GetRocksVersionAsString(true));
  gflags::SetUsageMessage(USAGE);
  if (argc < 2) {
    gflags::ShowUsageWithFlagsRestrict(argv[0], "tools/cf_repairer");
    exit(0);
  }

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  CfRepairer repairer;
  repairer.Run(argc, argv);
  return 0;
}
