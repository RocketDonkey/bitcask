#include "bitcask.h"

#include <chrono>
#include <exception>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace rd::bitcask {
namespace {

// Value written when a value is tombstoned. Keys with this value will be
// ignored the next time the cask is loaded.
constexpr std::string_view kTombstoneValue = "rdbc_tombstone";

// Suffix given to Bitcask files.
constexpr std::string_view kCaskSuffix = ".cask";

// Reads bytes from `input` into `target.
//
// NOTE: this/the overload below do not check for eof(), e.g.:
//   if (input.read((char*)target, sizeof(target)).eof()) {
//     // Handle.
//   }
// This is because checking the CRC should ensure the data is valid, but
// naturally that isn't implemented yet.
template <typename T>
void ReadToTarget(std::istream& input, T* target) {
  static_assert(!std::is_same_v<std::string, T>,
                "Use the ReadToTarget overload that takes a std::string");

  input.read((char*)target, sizeof(target));
}

// String-specific ReadToTarget overload that writes `size` bytes from `input`
// to `target.
//
// `target` is cleared before writing.
void ReadToTarget(std::istream& input, std::string* target, size_t size) {
  target->clear();
  target->resize(size);
  input.read(target->data(), size);
}

template <typename T>
void WriteToTarget(std::ostream& output, T* target) {
  output.write((char*)target, sizeof(target));
}

int64_t NowToMicros() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}
}  // namespace

namespace fs = std::filesystem;

std::streamoff Bitcask::CaskEntry::ValueOffset() {
  // TODO: CRC.
  return std::streamoff(sizeof(timestamp)) + std::streamoff(sizeof(key_sz)) +
         std::streamoff(sizeof(value_sz)) + std::streamoff(key.size());
}

// Note that reading/writing the data is not platform-independent and may
// break. Fortunately, this will have zero users so the risk of brekage is low.
//
// 💡: Never knew the correct name - this is the 'extraction' operator, '<<' is
// the 'insertion' operator.
std::istream& operator>>(std::istream& input, Bitcask::CaskEntry& cask_entry) {
  // 💡: input.eof() is set when a byte *beyond* EOF is read. In the below
  // sequence, if all records have been processed and we are now at EOF, this
  // still runs. However all reads 'fail' because the input is already at EOF.
  // `input` converts to boolean `false` when `eofbit` is set, which in turn
  // ends the `while` loop that calls this.
  //
  // Returning `input` as soon as EOF is reached would avoid some unnecessary
  // calls, but it would ruin the single-line approach below (appearance always
  // trumps performance).
  ReadToTarget(input, &cask_entry.timestamp);
  ReadToTarget(input, &cask_entry.key_sz);
  ReadToTarget(input, &cask_entry.value_sz);
  ReadToTarget(input, &cask_entry.key, cask_entry.key_sz);
  ReadToTarget(input, &cask_entry.value, cask_entry.value_sz);

  return input;
}

// Serializes `cask_entry` to `output`.
std::ostream& operator<<(std::ostream& output, Bitcask::CaskEntry& cask_entry) {
  WriteToTarget(output, &cask_entry.timestamp);
  WriteToTarget(output, &cask_entry.key_sz);
  WriteToTarget(output, &cask_entry.value_sz);

  output << cask_entry.key;
  output << cask_entry.value;

  return output;
}

Bitcask Bitcask::Open(const std::string& directory_name) {
  fs::path cask_path(directory_name);

  if (!fs::exists(cask_path)) {
    fs::create_directory(cask_path);
  }

  // Read all .cask files to build the KeyDir.
  rd::bitcask::Bitcask::KeyDirMap key_dir;
  for (const auto& file_entry : fs::directory_iterator(cask_path)) {
    if (file_entry.path().extension() != kCaskSuffix) {
      continue;
    }

    const fs::path& cask_file_path = file_entry.path();
    std::ifstream cask_file(cask_file_path, std::ios::binary);

    std::streampos entry_start;
    CaskEntry entry;
    while (entry_start = cask_file.tellg(), cask_file >> entry) {
      // Skip outdated entries.
      auto existing_key = key_dir.find(entry.key);
      if (existing_key != key_dir.end() &&
          existing_key->second.timestamp >= entry.timestamp) {
        continue;
      }

      // Prune tombstoned entities. Note that this reflects the true order of
      // operations - if an entry exists, this removes it but a subsequent
      // operation is free to re-add it.
      if (entry.value == kTombstoneValue) {
        key_dir.erase(entry.key);
        continue;
      }

      key_dir[entry.key] = {
          .file_id = cask_file_path,
          .value_sz = entry.value_sz,
          .value_pos = entry_start + entry.ValueOffset(),
          .timestamp = entry.timestamp,
      };
    }
  }

  // A new file is always created on startup. For now, just use the timestamp
  // as an identifier.
  std::string ts = std::to_string(NowToMicros());
  fs::path db_path = cask_path / ts.append(kCaskSuffix);
  return Bitcask(db_path, key_dir);
}

Bitcask::Bitcask(fs::path path, KeyDirMap key_dir)
    : db_path_(std::move(path)), key_dir_(std::move(key_dir)) {
  // 💡: opening in `out` without `app` truncates the file.
  f_ = std::make_unique<std::ofstream>(db_path_,
                                       std::ios::binary | std::ios::trunc);
}

Bitcask::~Bitcask() { f_->flush(); }

void Bitcask::Put(const std::string& key, std::string value) {
  int64_t time_us = NowToMicros();

  size_t value_size = value.length();

  CaskEntry cask_entry;
  cask_entry.timestamp = time_us;
  cask_entry.key_sz = key.length();
  cask_entry.value_sz = value_size;
  cask_entry.key = key;
  cask_entry.value = value;

  // Calculate the value offset before writing the entry (which will advance
  // the position).
  auto value_pos = f_->tellp() + cask_entry.ValueOffset();

  *f_ << cask_entry;
  f_->flush();

  key_dir_[key] = {
      .file_id = db_path_,
      .value_sz = value_size,
      .value_pos = value_pos,
      .timestamp = time_us,
  };
}

std::string Bitcask::Get(const std::string& key) const {
  auto itr = key_dir_.find(key);
  if (itr == key_dir_.end()) {
    throw MissingKeyException(key);
    return std::string{};
  }

  const auto& [entry_key, key_dir_entry] = *itr;

  // Load the corresponding file / value.
  std::ifstream input(key_dir_entry.file_id, std::ios::binary);
  input.seekg(key_dir_entry.value_pos);

  char value_data[key_dir_entry.value_sz];
  input.read(value_data, key_dir_entry.value_sz);
  std::string value(value_data, key_dir_entry.value_sz);

  return value;
}

void Bitcask::Delete(const std::string& key) {
  auto itr = key_dir_.find(key);
  if (itr == key_dir_.end()) {
    return;
  }

  // Tombstone the entry so it is cleared on the next merge.
  Put(key, std::string(kTombstoneValue));

  // Remove from the KeyDir so Get()'s fail.
  key_dir_.erase(key);
}

std::vector<std::string> Bitcask::ListKeys() const {
  std::vector<std::string> keys;
  keys.reserve(key_dir_.size());

  for (const auto& [key, value] : key_dir_) {
    keys.push_back(key);
  }
  return keys;
}

}  // namespace rd::bitcask
