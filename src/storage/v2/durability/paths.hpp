#pragma once

#include <cstdint>
#include <string>

#include "utils/timestamp.hpp"

namespace storage::durability {

static const std::string kSnapshotDirectory{"snapshots"};
static const std::string kWalDirectory{"wal"};
static const std::string kBackupDirectory{".backup"};
static const std::string kLockFile{".lock"};

// This is the prefix used for Snapshot and WAL filenames. It is a timestamp
// format that equals to: YYYYmmddHHMMSSffffff
const std::string kTimestampFormat =
    "{:04d}{:02d}{:02d}{:02d}{:02d}{:02d}{:06d}";

// Generates the name for a snapshot in a well-defined sortable format with the
// start timestamp appended to the file name.
inline std::string MakeSnapshotName(uint64_t start_timestamp) {
  std::string date_str = utils::Timestamp::Now().ToString(kTimestampFormat);
  return date_str + "_timestamp_" + std::to_string(start_timestamp);
}

// Generates the name for a WAL file in a well-defined sortable format.
inline std::string MakeWalName() {
  std::string date_str = utils::Timestamp::Now().ToString(kTimestampFormat);
  return date_str + "_current";
}

// Generates the name for a WAL file in a well-defined sortable format with the
// range of timestamps contained [from, to] appended to the name.
inline std::string RemakeWalName(const std::string &current_name,
                                 uint64_t from_timestamp,
                                 uint64_t to_timestamp) {
  return current_name.substr(0, current_name.size() - 8) + "_from_" +
         std::to_string(from_timestamp) + "_to_" + std::to_string(to_timestamp);
}

}  // namespace storage::durability
