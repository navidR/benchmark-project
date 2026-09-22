#pragma once

#include <boost/json/object.hpp>
#include <filesystem>
#include <functional>
#include <map>
#include <mutex>
#include <stop_token>

#include "bbp/drivers/chain_block.h"

namespace bbp {

boost::json::object ChainViewPageSchema();
boost::json::object ChainBlockSummaryJson(const ChainBlockSummary& block);
boost::json::object ChainBlockDetailJson(const ChainBlockDetail& detail);

// Each reader leases one explicitly identified runtime node for the query.
struct ChainBlockReader {
  std::string node_id;
  std::function<ChainBlockSummary(std::stop_token)> tip;
  std::function<ChainBlockSummary(std::uint64_t, std::stop_token)> summary;
  std::function<ChainBlockDetail(const std::string&, std::stop_token)> detail;
};

struct ChainViewRequest {
  std::optional<std::uint64_t> first_height;
  std::optional<std::uint64_t> selected_height;
  std::uint32_t limit = 16;
  bool operator==(const ChainViewRequest&) const = default;
};

// Synchronous, cancellation-aware shared read boundary. The TUI calls it from
// its own worker; MCP uses its existing operation workers. No UI thread RPC.
class ChainViewService {
 public:
  using Readers = std::function<std::vector<ChainBlockReader>()>;
  static constexpr std::size_t kMaximumEntries = 64;
  static constexpr std::size_t kMaximumDetailBytes = 2 * 1024 * 1024;
  explicit ChainViewService(std::filesystem::path run_root,
                            Readers readers = {});
  ~ChainViewService();
  boost::json::object Query(const ChainViewRequest& request,
                            std::stop_token stop = {});
  void SetObserver(std::function<void(const boost::json::object&)> observer);
  void Close();

 private:
  struct Entry {
    ChainBlockSummary summary;
    boost::json::object detail;
    std::uint64_t used = 0;
  };
  void Load(std::stop_token stop);
  void Save(std::stop_token stop);
  void Trim();
  void UpdateTip(ChainBlockReader& reader, const ChainBlockSummary& next,
                 std::stop_token stop);
  std::filesystem::path path_;
  Readers readers_;
  std::function<void(const boost::json::object&)> observer_;
  std::timed_mutex mutex_;
  std::stop_source closing_;
  std::map<std::uint64_t, Entry> entries_;
  std::optional<ChainBlockSummary> tip_;
  std::string source_;
  std::string notice_;
  std::uint64_t used_ = 0;
  bool loaded_ = false;
  bool dirty_ = false;
};

}  // namespace bbp
