#pragma once

#include <boost/json/object.hpp>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <stop_token>
#include <string>
#include <string_view>

namespace bbp {

constexpr std::size_t kMaximumRunReportSummaryRecords = 256U;

struct RunReportRefreshStats {
  std::uint64_t event_records = 0;
  std::uint64_t metric_records = 0;
  std::uint64_t wallet_metric_records = 0;
  bool has_backlog = false;
};

class IncrementalRunReport {
 public:
  explicit IncrementalRunReport(const std::filesystem::path& run_root,
                                std::stop_token stop_token = {});
  ~IncrementalRunReport();

  IncrementalRunReport(IncrementalRunReport&&) noexcept;
  IncrementalRunReport& operator=(IncrementalRunReport&&) noexcept;
  IncrementalRunReport(const IncrementalRunReport&) = delete;
  IncrementalRunReport& operator=(const IncrementalRunReport&) = delete;

  const boost::json::object& Refresh(
      std::size_t maximum_records_per_file =
          std::numeric_limits<std::size_t>::max(),
      std::stop_token stop_token = {});
  const RunReportRefreshStats& last_refresh_stats() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

boost::json::object BuildRunReport(const std::filesystem::path& run_root,
                                   std::stop_token stop_token = {});
std::string BuildRunReportJson(const std::filesystem::path& run_root,
                               std::stop_token stop_token = {});
std::string BuildNodeReportJson(const std::filesystem::path& run_root,
                                std::string_view node_id,
                                std::uint64_t operator_command_sequence,
                                std::stop_token stop_token = {});

}  // namespace bbp
