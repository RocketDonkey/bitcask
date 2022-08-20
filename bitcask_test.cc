#include "bitcask.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <gtest/internal/gtest-internal.h>

#include <filesystem>
#include <sstream>
#include <string>

// NOTE: The tests are a little light but stick to the public interface. More
// robusts tests may be added if this is ever used in an industrial setting...

namespace rd::bitcask {
namespace {

namespace fs = ::std::filesystem;

using ::testing::TempDir;
using ::testing::TestInfo;
using ::testing::Throws;
using ::testing::UnorderedElementsAre;

class BitcaskTest : public testing::Test {
 protected:
  BitcaskTest() {
    const testing::TestInfo* const test_info =
        testing::UnitTest::GetInstance()->current_test_info();

    cask_dir_ =
        fs::path(TempDir()) / test_info->test_case_name() / test_info->name();
    fs::create_directories(fs::path(cask_dir_));
  }

  ~BitcaskTest() { fs::remove_all(cask_dir_); }

  fs::path cask_dir_;
};

TEST_F(BitcaskTest, PutsAndGets) {
  auto bc = Bitcask::Open(cask_dir_);

  bc.Put("Hello", "val");
  bc.Put("123", "something");
  bc.Put("", "empty");

  EXPECT_EQ(bc.Get("Hello"), "val");
  EXPECT_EQ(bc.Get("123"), "something");
  EXPECT_EQ(bc.Get(""), "empty");
  EXPECT_THAT([&]() { bc.Get("huh??"); }, Throws<MissingKeyException>());

  // Update an existing key.
  bc.Put("Hello", "new_val");
  EXPECT_EQ(bc.Get("Hello"), "new_val");
}

TEST_F(BitcaskTest, Deletes) {
  auto bc = Bitcask::Open(cask_dir_);

  bc.Put("Hello", "val");
  EXPECT_EQ(bc.Get("Hello"), "val");

  bc.Delete("Hello");

  EXPECT_THAT([&]() { bc.Get("Hello"); }, Throws<MissingKeyException>());
}

TEST_F(BitcaskTest, IgnoresTombstonedEntriesOnLoad) {
  // Create a cask with a few values and delete one.
  {
    auto bc = Bitcask::Open(cask_dir_);

    bc.Put("Hello", "val");
    bc.Put("Goodbye", "another_val");
    bc.Put("Goodbye", "maybe_now");
    bc.Put("Goodbye", "this will for sure outlive us all");
    bc.Put("Goodbye", "still here!");
    EXPECT_EQ(bc.Get("Goodbye"), "still here!");

    bc.Delete("Goodbye");
    EXPECT_THAT([&]() { bc.Get("Goodbye"); }, Throws<MissingKeyException>());
  }

  // Open a new cask / ensure the deleted keys are gone.
  auto bc = Bitcask::Open(cask_dir_);

  EXPECT_EQ(bc.Get("Hello"), "val");
  EXPECT_THAT([&]() { bc.Get("Goodbye"); }, Throws<MissingKeyException>());
}

TEST_F(BitcaskTest, LoadsFromMultipleFiles) {
  constexpr int num_casks = 10;

  // Add each key/value to a new file.
  for (int i = 0; i < num_casks; ++i) {
    // Create a new Bitcask instance (using the same root) for each key/value.
    auto bc = Bitcask::Open(cask_dir_);
    bc.Put("key_" + std::to_string(i), std::to_string(i));
  }
  // Ensure the correct number of files was created.
  int cask_count = 0;
  for (const auto& file_entry : fs::directory_iterator(cask_dir_)) {
    if (file_entry.path().extension() == ".cask") {
      cask_count++;
    }
  }

  EXPECT_EQ(cask_count, num_casks);

  // Open a single instance and ensure all values can be found.
  auto bc = Bitcask::Open(cask_dir_);
  for (int i = 0; i < num_casks; ++i) {
    const std::string expected_value = std::to_string(i);
    EXPECT_EQ(bc.Get("key_" + expected_value), expected_value);
  }
}

TEST_F(BitcaskTest, ListKeys) {
  auto bc = Bitcask::Open(cask_dir_);

  bc.Put("Hello", "val");
  bc.Put("123", "something");
  bc.Put("123", "updated_value");
  bc.Put("", "empty");

  std::vector<std::string> keys = bc.ListKeys();

  EXPECT_THAT(keys, UnorderedElementsAre("Hello", "123", ""));
}

}  // namespace
}  // namespace rd::bitcask
