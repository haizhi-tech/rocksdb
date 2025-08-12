// To statically build cf_repairer:
//
// mkdir build; cd build
// cmake .. -DCMAKE_EXPORT_COMPILE_COMMANDS=1 -DROCKSDB_BUILD_SHARED=OFF -DBUILD_SHARED_LIBS=OFF -DWITH_GFLAGS=1
// make cf_repairer -j 16

#include "util/gflags_compat.h"
#include "rocksdb/version.h"

DEFINE_string(db_path, "", "The path of db to operate with");
DEFINE_string(command, "", "The command to run");
DEFINE_int32(workers, 1, "The number of worker to run repair process");

class CfRepairer {
    private:
        void ShowHelp();
    public:
        void Run(int argc, char** argv);
};

void CfRepairer::ShowHelp() {
}

void CfRepairer::Run(int argc, char **argv) {
    gflags::SetVersionString(rocksdb::GetRocksVersionAsString(true));
    gflags::SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) + " [OPTIONS]...");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc < 2) {
        gflags::ShowUsageWithFlagsRestrict(argv[0], "tools/cf_repairer");
    }
}

int main(int argc, char** argv) {
    CfRepairer repairer;
    repairer.Run(argc, argv);
    return 0;
}
