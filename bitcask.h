// Toy implementation of a Bitcask key/value store.
//
// Paper: https://riak.com/assets/bitcask-intro.pdf
//
// Nothing here should ever be used for anything - this is just tinkering
// around with implementing Bitcask in C++.

#include <filesystem>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace rd::bitcask {

// Exception thrown when trying to access a key that doesn't exist.
//
// Using a StatusOr is a potentially better alternative, but may as well try
// out my first exception.
struct MissingKeyException : public std::exception {
 public:
  explicit MissingKeyException(const std::string& key) {
    std::stringstream message;
    message << "Key '" << key << "' not found";
    message_ = message.str();
  }
  const char* what() const throw() { return message_.c_str(); }

 private:
  std::string message_;
};

// `Bitcask` manages all operations on the underlying data.
class Bitcask {
 public:
  ~Bitcask();

  // Opens a new/existing Bitcask rooted at `directory_name`.
  //
  // TODO: Support options (e.g., opening read/write casks).
  //
  // Note that calling this creates a new (empty) file. Existing Bitcask files
  // in `directory_name` (e.g., from an old process that was shut down) are
  // loaded into the Bitcask before it is returned.
  static Bitcask Open(const std::string& directory_name);

  // Stores `key` with `value` in the Bitcask.
  void Put(const std::string& key, std::string value);

  // Retrieves the value associated with `key`.
  std::string Get(const std::string& key) const;

  // Deletes the value associated with `key`.
  void Delete(const std::string& key);

  // List all of the keys in this Bitcask.
  std::vector<std::string> ListKeys() const;

 private:
  // Value piece of the KeyDir hash table.
  //
  // The KeyDir maps every key to this fixed-size structure that can be used to
  // find the value.
  struct KeyDirEntry {
    // Unique ID of the file containing this entry (in this implementation,
    // just a file path).
    std::string file_id;
    // Size of the value referenced by this key.
    size_t value_sz;
    // Offset of the value within `file_id`.
    std::streampos value_pos;
    // Timestamp of when this entry was created. This allows pruning/expiring
    // older entries when loading existing Bitcask files.
    int64_t timestamp;
  };

  // A single entry within the Bitcask.
  struct CaskEntry {
    // TODO: CRC
    int64_t timestamp = 0;
    size_t key_sz = 0;
    size_t value_sz = 0;
    std::string key;
    std::string value;

    // Human-readable representation of this entry.
    std::string DebugString() const {
      std::stringstream ss;
      ss << "CaskEntry(" << timestamp << ", key_size:" << key_sz
         << ", value_sz:" << value_sz << ", key:" << key << ", value:" << value
         << ")\n";
      return ss.str();
    }

    // Helper for calculating this entry's value's offset in the underlying
    // file (useful when writing the entry to the KeyDir).
    std::streamoff ValueOffset();
  };

  // Reading from / serializing to files.
  friend std::istream& operator>>(std::istream& input, CaskEntry& cask_entry);
  friend std::ostream& operator<<(std::ostream& output, CaskEntry& cask_entry);

  using KeyDirMap = std::unordered_map<std::string, KeyDirEntry>;

  // Constructs a new Bitcask at `path` with a pre-populated `key_dir`.
  explicit Bitcask(std::filesystem::path path, KeyDirMap key_dir);

  std::filesystem::path db_path_;
  std::unique_ptr<std::ofstream> f_;
  KeyDirMap key_dir_;
};

}  // namespace rd::bitcask
