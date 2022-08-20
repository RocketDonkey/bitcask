# Bitcask CMake.
#
# Command:
#   $ cmake -S . -B build
#
# 💡 Running `cmake -v` dumps the build output, and the final step invoked a
# `c++` command. Turns out this is an alternative (per `ls -l /usr/bin/c++`) to
# `g++`. Turns out this is why `vi` always (for me) mapped to `vim`.
# `update-alternatives --get-selections` shows the existing mappings.

cmake_minimum_required(VERSION 3.10)

project(Bitcask)

# Generate compile_commands.json. Among other things, this is used by YCM.
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/c0e032efe587efa99413dd9ac54c3953a7915588.zip
)
FetchContent_MakeAvailable(googletest)

add_library(bitcask bitcask.cc)

add_executable(main main.cc)

target_link_libraries(main PUBLIC bitcask)

target_include_directories(main PUBLIC
                          "${PROJECT_BINARY_DIR}"
                          "${PROJECT_SOURCE_DIR}"
                          )

# Installs the tool to /usr/local/bin
# install(TARGETS main DESTINATION bin)
enable_testing()

add_executable(
  bitcask_test
  bitcask_test.cc
)

target_link_libraries(
  bitcask_test
  gtest_main
  bitcask
  # 💡 Including this was required to get #include <gmock/gmock.h> to work.
  # The reason is that this is building the `bitcask_test` target
  gmock
)

include(GoogleTest)
gtest_discover_tests(bitcask_test)

# Pass flags directly to the generated test (e.g., --gtest_repeat=100), but
# should figure out how to do this automatically?
