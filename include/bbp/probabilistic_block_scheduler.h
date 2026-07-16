#pragma once

#include <condition_variable>
#include <exception>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "bbp/block_production_policy.h"

namespace bbp {

class ProbabilisticBlockScheduler {
 public:
  using ProductionHandler = std::function<void(const std::string&)>;
  using FailureHandler =
      std::function<void(const std::string&, std::string_view)>;

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
