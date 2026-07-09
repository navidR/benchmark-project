#include "benchmark_sim/log_tail.h"
#include "benchmark_sim/util.h"

#include <unistd.h>

#include <filesystem>
#include <string>

#include <boost/test/unit_test.hpp>

namespace {

std::filesystem::path MakeTestDir(const std::string& name) {
  const std::filesystem::path dir =
      std::filesystem::temp_directory_path() /
      ("benchmark-sim-" + name + "-" + std::to_string(getpid()));
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  return dir;
}

}  // namespace

BOOST_AUTO_TEST_CASE(log_tail_reads_incrementally_by_offset) {
  const std::filesystem::path dir = MakeTestDir("log-tail-incremental");
  const std::filesystem::path log = dir / "daemon.log";
  bsim::WriteText(log, "alpha\nbeta\n");

  bsim::LogTailChunk first = bsim::TailLogFile(log, 0, 1024);
  BOOST_TEST(first.start_offset == 0U);
  BOOST_TEST(first.next_offset == 11U);
  BOOST_TEST(!first.truncated);
  BOOST_TEST(!first.offset_reset);
  BOOST_TEST(first.text == "alpha\nbeta\n");

  bsim::AppendLine(log, "gamma");
  bsim::LogTailChunk second = bsim::TailLogFile(log, first.next_offset, 1024);
  BOOST_TEST(second.start_offset == 11U);
  BOOST_TEST(second.next_offset == 17U);
  BOOST_TEST(second.text == "gamma\n");

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(log_tail_reports_truncated_bounded_reads) {
  const std::filesystem::path dir = MakeTestDir("log-tail-bounded");
  const std::filesystem::path log = dir / "daemon.log";
  bsim::WriteText(log, "0123456789");

  bsim::LogTailChunk chunk = bsim::TailLogFile(log, 0, 4);
  BOOST_TEST(chunk.start_offset == 0U);
  BOOST_TEST(chunk.next_offset == 4U);
  BOOST_TEST(chunk.truncated);
  BOOST_TEST(chunk.text == "0123");

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(log_tail_resets_offset_after_file_truncation) {
  const std::filesystem::path dir = MakeTestDir("log-tail-reset");
  const std::filesystem::path log = dir / "daemon.log";
  bsim::WriteText(log, "first long contents\n");
  const bsim::LogTailChunk before_truncate = bsim::TailLogFile(log, 0, 1024);
  bsim::WriteText(log, "new\n");

  bsim::LogTailChunk after_truncate =
      bsim::TailLogFile(log, before_truncate.next_offset, 1024);
  BOOST_TEST(after_truncate.offset_reset);
  BOOST_TEST(after_truncate.start_offset == 0U);
  BOOST_TEST(after_truncate.next_offset == 4U);
  BOOST_TEST(after_truncate.text == "new\n");

  std::filesystem::remove_all(dir);
}
