#include <unistd.h>

#include <boost/test/unit_test.hpp>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "bbp/log_tail.h"
#include "bbp/log_view.h"
#include "bbp/util.h"

namespace {

std::filesystem::path MakeTestDir(const std::string& name) {
  const std::filesystem::path dir =
      std::filesystem::temp_directory_path() /
      ("bbp-" + name + "-" + std::to_string(getpid()));
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  return dir;
}

}  // namespace

BOOST_AUTO_TEST_CASE(log_tail_reads_incrementally_by_offset) {
  const std::filesystem::path dir = MakeTestDir("log-tail-incremental");
  const std::filesystem::path log = dir / "daemon.log";
  bbp::WriteText(log, "alpha\nbeta\n");

  const std::optional<bbp::LogTailChunk> first_result =
      bbp::TailLogFile(log, {}, 1024);
  BOOST_REQUIRE(first_result.has_value());
  const bbp::LogTailChunk& first = *first_result;
  BOOST_TEST(first.start_offset == 0U);
  BOOST_TEST(first.next_offset == 11U);
  BOOST_TEST(!first.truncated);
  BOOST_TEST(!first.offset_reset);
  BOOST_TEST(first.text == "alpha\nbeta\n");

  bbp::AppendLine(log, "gamma");
  const std::optional<bbp::LogTailChunk> second_result =
      bbp::TailLogFile(log, first.next_cursor, 1024);
  BOOST_REQUIRE(second_result.has_value());
  const bbp::LogTailChunk& second = *second_result;
  BOOST_TEST(second.start_offset == 11U);
  BOOST_TEST(second.next_offset == 17U);
  BOOST_TEST(second.text == "gamma\n");

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(log_tail_reports_truncated_bounded_reads) {
  const std::filesystem::path dir = MakeTestDir("log-tail-bounded");
  const std::filesystem::path log = dir / "daemon.log";
  bbp::WriteText(log, "0123456789");

  const std::optional<bbp::LogTailChunk> result = bbp::TailLogFile(log, {}, 4);
  BOOST_REQUIRE(result.has_value());
  const bbp::LogTailChunk& chunk = *result;
  BOOST_TEST(chunk.start_offset == 0U);
  BOOST_TEST(chunk.next_offset == 4U);
  BOOST_TEST(chunk.truncated);
  BOOST_TEST(chunk.text == "0123");

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(log_tail_resets_offset_after_file_truncation) {
  const std::filesystem::path dir = MakeTestDir("log-tail-reset");
  const std::filesystem::path log = dir / "daemon.log";
  bbp::WriteText(log, "first long contents\n");
  const std::optional<bbp::LogTailChunk> before_truncate_result =
      bbp::TailLogFile(log, {}, 1024);
  BOOST_REQUIRE(before_truncate_result.has_value());
  const bbp::LogTailChunk& before_truncate = *before_truncate_result;
  bbp::WriteText(log, "new\n");

  const std::optional<bbp::LogTailChunk> after_truncate_result =
      bbp::TailLogFile(log, before_truncate.next_cursor, 1024);
  BOOST_REQUIRE(after_truncate_result.has_value());
  const bbp::LogTailChunk& after_truncate = *after_truncate_result;
  BOOST_TEST(after_truncate.offset_reset);
  BOOST_TEST(after_truncate.start_offset == 0U);
  BOOST_TEST(after_truncate.next_offset == 4U);
  BOOST_TEST(after_truncate.text == "new\n");

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(log_tail_detects_truncate_and_regrow_on_same_inode) {
  const std::filesystem::path dir = MakeTestDir("log-tail-regrow");
  const std::filesystem::path log = dir / "daemon.log";
  bbp::WriteText(log, "old contents before restart\n");
  const std::optional<bbp::LogTailChunk> before =
      bbp::TailLogFile(log, {}, 1024);
  BOOST_REQUIRE(before.has_value());

  bbp::WriteText(log,
                 "new process contents are longer than the previous log\n");
  const std::optional<bbp::LogTailChunk> after =
      bbp::TailLogFile(log, before->next_cursor, 1024);
  BOOST_REQUIRE(after.has_value());
  BOOST_TEST(after->offset_reset);
  BOOST_TEST(after->start_offset == 0U);
  BOOST_TEST(after->text ==
             "new process contents are longer than the previous log\n");

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(log_tail_returns_empty_for_missing_file) {
  const std::filesystem::path missing =
      MakeTestDir("log-tail-missing") / "missing.log";
  BOOST_TEST(!bbp::TailLogFile(missing, {}, 1024).has_value());
  std::filesystem::remove_all(missing.parent_path());
}

BOOST_AUTO_TEST_CASE(recent_log_view_reads_only_the_requested_tail_lines) {
  const std::filesystem::path dir = MakeTestDir("recent-log-view");
  const std::filesystem::path log = dir / "simulator.log";
  std::string contents(128U * 1024U, 'x');
  contents += "\nfirst\nsecond\nthird\nfourth";
  bbp::WriteText(log, contents);

  const std::vector<std::string> lines = bbp::ReadRecentLogLines(log, 3U);
  const std::vector<std::string> expected = {"second", "third", "fourth"};
  BOOST_TEST(lines == expected, boost::test_tools::per_element());
  BOOST_TEST(bbp::ReadRecentLogLines(log, 0U).empty());

  std::filesystem::remove_all(dir);
}
