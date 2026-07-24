#pragma once

#include <condition_variable>
#include <exception>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "bbp/block_production_policy.h"

namespace bbp {

class ProbabilisticBlockScheduler {
 public:
  using ProductionHandler = std::function<void(const std::string&)>;
  using FailureHandler =
      std::function<void(const std::string&, std::string_view)>;

  class PreparedAdd {
   public:
    PreparedAdd(PreparedAdd&&) noexcept = default;
    PreparedAdd& operator=(PreparedAdd&&) noexcept = default;

    PreparedAdd(const PreparedAdd&) = delete;
    PreparedAdd& operator=(const PreparedAdd&) = delete;

    void Commit() noexcept;

   private:
    friend class ProbabilisticBlockScheduler;

    PreparedAdd(ProbabilisticBlockScheduler* owner,
                std::unique_lock<std::mutex> lock,
                std::vector<std::string> miner_node_ids,
                std::vector<bool> active_miners,
                std::vector<bool> in_flight_miners)
        : owner_(owner),
          lock_(std::move(lock)),
          miner_node_ids_(std::move(miner_node_ids)),
          active_miners_(std::move(active_miners)),
          in_flight_miners_(std::move(in_flight_miners)) {}

    ProbabilisticBlockScheduler* owner_ = nullptr;
    std::unique_lock<std::mutex> lock_;
    std::vector<std::string> miner_node_ids_;
    std::vector<bool> active_miners_;
    std::vector<bool> in_flight_miners_;
  };

  ProbabilisticBlockScheduler(std::vector<std::string> miner_node_ids,
                              BlockProductionPolicy policy,
                              ProductionHandler production_handler,
                              FailureHandler failure_handler);
  ProbabilisticBlockScheduler(const ProbabilisticBlockScheduler&) = delete;
  ProbabilisticBlockScheduler& operator=(const ProbabilisticBlockScheduler&) =
      delete;
  ~ProbabilisticBlockScheduler();

  void Start();
  void Stop();
  PreparedAdd PrepareAddMiners(std::vector<std::string> node_ids);
  void StartMiner(const std::string& node_id);
  bool StopMiner(const std::string& node_id);
  void UpdatePolicy(BlockProductionPolicy policy);

 private:
  void Run();

  std::vector<std::string> miner_node_ids_;
  std::vector<bool> active_miners_;
  std::vector<bool> in_flight_miners_;
  BlockProductionPolicy policy_;
  ProductionHandler production_handler_;
  FailureHandler failure_handler_;
  std::mutex mutex_;
  std::condition_variable condition_;
  std::thread thread_;
  std::exception_ptr failure_;
  bool stop_requested_ = false;
  bool policy_changed_ = false;
  bool started_ = false;
};

}  // namespace bbp
