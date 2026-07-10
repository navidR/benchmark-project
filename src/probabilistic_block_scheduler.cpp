#include "bbp/probabilistic_block_scheduler.h"

#include <algorithm>
#include <boost/random/bernoulli_distribution.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <chrono>
#include <stdexcept>
#include <utility>

namespace bbp {

ProbabilisticBlockScheduler::ProbabilisticBlockScheduler(
    std::vector<std::string> miner_node_ids, BlockProductionPolicy policy,
    ProductionHandler production_handler, FailureHandler failure_handler)
    : miner_node_ids_(std::move(miner_node_ids)),
      active_miners_(miner_node_ids_.size(), true),
      in_flight_miners_(miner_node_ids_.size(), false),
      policy_(policy),
      production_handler_(std::move(production_handler)),
      failure_handler_(std::move(failure_handler)) {
  if (miner_node_ids_.empty()) {
    throw std::runtime_error(
        "probabilistic block scheduler requires at least one miner");
  }
  if (std::any_of(miner_node_ids_.begin(), miner_node_ids_.end(),
                  [](const std::string& node_id) { return node_id.empty(); })) {
    throw std::runtime_error("block scheduler miner ids cannot be empty");
  }
  if (!production_handler_) {
    throw std::runtime_error("block scheduler requires a production handler");
  }
  if (!failure_handler_) {
    throw std::runtime_error("block scheduler requires a failure handler");
  }
}

ProbabilisticBlockScheduler::~ProbabilisticBlockScheduler() {
  try {
    Stop();
  } catch (...) {
  }
}

void ProbabilisticBlockScheduler::Start() {
  if (started_) {
    throw std::runtime_error(
        "probabilistic block scheduler is already started");
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_requested_ = false;
    policy_changed_ = false;
    failure_ = nullptr;
  }
  started_ = true;
  thread_ = std::thread(&ProbabilisticBlockScheduler::Run, this);
}

void ProbabilisticBlockScheduler::Stop() {
  if (!started_) {
    return;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_requested_ = true;
  }
  condition_.notify_all();
  if (thread_.joinable()) {
    thread_.join();
  }
  started_ = false;
  std::exception_ptr failure;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    failure = std::exchange(failure_, nullptr);
  }
  if (failure) {
    std::rethrow_exception(failure);
  }
}

void ProbabilisticBlockScheduler::StopMiner(const std::string& node_id) {
  std::unique_lock<std::mutex> lock(mutex_);
  const auto miner =
      std::find(miner_node_ids_.begin(), miner_node_ids_.end(), node_id);
  if (miner == miner_node_ids_.end()) {
    throw std::runtime_error("node is not a configured miner: " + node_id);
  }
  const std::size_t index =
      static_cast<std::size_t>(std::distance(miner_node_ids_.begin(), miner));
  active_miners_[index] = false;
  condition_.wait(lock, [this, index] { return !in_flight_miners_[index]; });
}

void ProbabilisticBlockScheduler::UpdatePolicy(BlockProductionPolicy policy) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    policy_ = policy;
    policy_changed_ = true;
  }
  condition_.notify_all();
}

void ProbabilisticBlockScheduler::Run() {
  std::uint64_t current_seed = 0U;
  std::chrono::steady_clock::time_point next_draw;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    current_seed = policy_.seed();
    next_draw = std::chrono::steady_clock::now() + policy_.period();
  }
  boost::random::mt19937_64 random(current_seed);
  while (true) {
    std::string node_id;
    std::size_t selected_index = 0U;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      if (stop_requested_) {
        return;
      }
      if (policy_changed_) {
        if (policy_.seed() != current_seed) {
          current_seed = policy_.seed();
          random.seed(current_seed);
        }
        next_draw = std::chrono::steady_clock::now() + policy_.period();
        policy_changed_ = false;
      } else {
        const auto now = std::chrono::steady_clock::now();
        if (next_draw <= now) {
          const auto skipped = (now - next_draw) / policy_.period() + 1;
          next_draw += policy_.period() * skipped;
        }
      }
      if (condition_.wait_until(lock, next_draw, [this] {
            return stop_requested_ || policy_changed_;
          })) {
        if (stop_requested_) {
          return;
        }
        continue;
      }
      if (stop_requested_) {
        return;
      }

      const BlockProductionPolicy policy = policy_;
      next_draw += policy.period();
      boost::random::bernoulli_distribution<double> produce(
          policy.probability());
      if (!produce(random)) {
        continue;
      }
      std::vector<std::size_t> active_indexes;
      active_indexes.reserve(active_miners_.size());
      for (std::size_t index = 0; index < active_miners_.size(); ++index) {
        if (active_miners_[index]) {
          active_indexes.push_back(index);
        }
      }
      if (active_indexes.empty()) {
        continue;
      }
      boost::random::uniform_int_distribution<std::size_t> select(
          0U, active_indexes.size() - 1U);
      selected_index = active_indexes[select(random)];
      in_flight_miners_[selected_index] = true;
      node_id = miner_node_ids_[selected_index];
    }
    std::exception_ptr failure;
    try {
      production_handler_(node_id);
    } catch (const std::exception& error) {
      try {
        failure_handler_(node_id, error.what());
      } catch (...) {
        failure = std::current_exception();
      }
    } catch (...) {
      try {
        failure_handler_(node_id, "unknown block production failure");
      } catch (...) {
        failure = std::current_exception();
      }
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      in_flight_miners_[selected_index] = false;
      if (failure) {
        failure_ = failure;
        stop_requested_ = true;
      }
    }
    condition_.notify_all();
    if (failure) {
      return;
    }
  }
}

}  // namespace bbp
