#pragma once

#include <boost/json/object.hpp>
#include <filesystem>
#include <functional>
#include <mutex>
#include <stop_token>

#include "bbp/drivers/chain_pool.h"

namespace bbp {
boost::json::object PoolTransactionJson(const ChainPoolTransaction& tx);
boost::json::object PoolSummaryJson(const ChainPoolSummary& summary);
boost::json::object PoolViewPageSchema();

struct PoolReader {
  std::string node_id;
  std::function<ChainPoolSnapshot(std::stop_token)> snapshot;
  std::function<ChainPoolTransaction(const ChainPoolTransaction&,
                                     std::stop_token)>
      detail;
  std::function<std::string(const std::string&, std::stop_token)> departure;
};
struct PoolViewRequest {
  std::string selected_id;
  std::uint32_t index = 0;
  std::uint32_t limit = 16;
  // Absent selects automatic sticky failover; a value pins exactly one node.
  std::optional<std::string> source_node = {};
  bool operator==(const PoolViewRequest&) const = default;
};

// One current snapshot and one selected detail; no unbounded history. Every
// reader leases its node generation. Close cancels RPC and releases callbacks.
class PoolViewService {
 public:
  using Readers = std::function<std::vector<PoolReader>()>;
  static constexpr std::size_t kMaximumBytes = 32 * 1024 * 1024;
  explicit PoolViewService(std::filesystem::path run_root,
                           Readers readers = {});
  ~PoolViewService();
  boost::json::object Query(const PoolViewRequest& request,
                            std::stop_token stop = {});
  void SetObserver(std::function<void(const boost::json::object&)> observer);
  void Close();

 private:
  std::filesystem::path path_;
  Readers readers_;
  std::function<void(const boost::json::object&)> observer_;
  std::timed_mutex mutex_;
  std::stop_source closing_;
  boost::json::object captured_;
  std::string notice_;
  bool loaded_ = false;
};
}  // namespace bbp
