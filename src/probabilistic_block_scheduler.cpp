#include "bbp/probabilistic_block_scheduler.h"

#include <algorithm>
#include <boost/random/bernoulli_distribution.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <chrono>
#include <set>
#include <stdexcept>
#include <utility>

#include "bbp/simulation_cancelled.h"

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

ProbabilisticBlockScheduler::PreparedAdd
ProbabilisticBlockScheduler::PrepareAddMiners(
    std::vector<std::string> node_ids) {
  return PrepareAddMiners(std::move(node_ids), true);
}

ProbabilisticBlockScheduler::PreparedAdd
ProbabilisticBlockScheduler::PrepareAddMinersInactive(
    std::vector<std::string> node_ids) {
  return PrepareAddMiners(std::move(node_ids), false);
}

ProbabilisticBlockScheduler::PreparedAdd
ProbabilisticBlockScheduler::PrepareAddMiners(std::vector<std::string> node_ids,
                                              bool initially_active) {
  if (node_ids.empty()) {
    throw std::invalid_argument(
        "block scheduler miner addition cannot be empty");
  }
  std::unique_lock<std::mutex> lock(mutex_);
  if (membership_mutation_pending_) {
    throw std::logic_error(
        "block scheduler membership mutation is already pending");
  }
  std::vector<std::string> next_ids = miner_node_ids_;
  std::vector<bool> next_active = active_miners_;
  std::vector<bool> next_in_flight = in_flight_miners_;
  next_ids.reserve(next_ids.size() + node_ids.size());
  next_active.reserve(next_active.size() + node_ids.size());
  next_in_flight.reserve(next_in_flight.size() + node_ids.size());
  for (std::string& node_id : node_ids) {
    if (node_id.empty()) {
      throw std::invalid_argument("block scheduler miner id cannot be empty");
    }
    if (std::find(next_ids.begin(), next_ids.end(), node_id) !=
        next_ids.end()) {
      throw std::invalid_argument("block scheduler miner is already present: " +
                                  node_id);
    }
    next_ids.push_back(std::move(node_id));
    next_active.push_back(initially_active);
    next_in_flight.push_back(false);
  }
  return PreparedAdd(this, std::move(lock), std::move(next_ids),
                     std::move(next_active), std::move(next_in_flight));
}

void ProbabilisticBlockScheduler::PreparedAdd::Commit() noexcept {
  if (owner_ == nullptr || !lock_.owns_lock() ||
      lock_.mutex() != &owner_->mutex_) {
    std::terminate();
  }
  owner_->miner_node_ids_.swap(miner_node_ids_);
  owner_->active_miners_.swap(active_miners_);
  owner_->in_flight_miners_.swap(in_flight_miners_);
  lock_.unlock();
  owner_->condition_.notify_all();
  owner_ = nullptr;
}

ProbabilisticBlockScheduler::PreparedRemove::PreparedRemove(
    PreparedRemove&& other) noexcept
    : owner_(std::exchange(other.owner_, nullptr)),
      lock_(std::move(other.lock_)),
      miner_node_ids_(std::move(other.miner_node_ids_)),
      active_miners_(std::move(other.active_miners_)),
      in_flight_miners_(std::move(other.in_flight_miners_)) {}

ProbabilisticBlockScheduler::PreparedRemove::~PreparedRemove() { Abandon(); }

void ProbabilisticBlockScheduler::PreparedRemove::Abandon() noexcept {
  if (owner_ == nullptr) {
    return;
  }
  if (!lock_.owns_lock() || lock_.mutex() != &owner_->mutex_ ||
      !owner_->membership_mutation_pending_) {
    std::terminate();
  }
  owner_->membership_mutation_pending_ = false;
  lock_.unlock();
  owner_->condition_.notify_all();
  owner_ = nullptr;
}

void ProbabilisticBlockScheduler::PreparedRemove::Commit() noexcept {
  if (owner_ == nullptr || !lock_.owns_lock() ||
      lock_.mutex() != &owner_->mutex_ ||
      !owner_->membership_mutation_pending_) {
    std::terminate();
  }
  owner_->miner_node_ids_.swap(miner_node_ids_);
  owner_->active_miners_.swap(active_miners_);
  owner_->in_flight_miners_.swap(in_flight_miners_);
  owner_->membership_mutation_pending_ = false;
  lock_.unlock();
  owner_->condition_.notify_all();
  owner_ = nullptr;
}

ProbabilisticBlockScheduler::PreparedRemove
ProbabilisticBlockScheduler::PrepareRemoveMiners(
    std::vector<std::string> node_ids, std::stop_token stop_token) {
  if (node_ids.empty()) {
    throw std::invalid_argument(
        "block scheduler miner removal cannot be empty");
  }
  const std::set<std::string> requested(node_ids.begin(), node_ids.end());
  if (requested.size() != node_ids.size() || requested.contains("")) {
    throw std::invalid_argument(
        "block scheduler miner removal must contain unique nonempty ids");
  }

  std::unique_lock<std::mutex> lock(mutex_);
  for (const std::string& node_id : requested) {
    if (std::find(miner_node_ids_.begin(), miner_node_ids_.end(), node_id) ==
        miner_node_ids_.end()) {
      throw std::invalid_argument("block scheduler miner is not configured: " +
                                  node_id);
    }
  }
  if (membership_mutation_pending_) {
    throw std::logic_error(
        "block scheduler membership mutation is already pending");
  }
  membership_mutation_pending_ = true;
  condition_.notify_all();
  const auto selected_in_flight = [&] {
    for (std::size_t index = 0U; index < miner_node_ids_.size(); ++index) {
      if (requested.contains(miner_node_ids_[index]) &&
          in_flight_miners_[index]) {
        return true;
      }
    }
    return false;
  };
  if (!condition_.wait(lock, stop_token,
                       [&] { return !selected_in_flight(); })) {
    membership_mutation_pending_ = false;
    lock.unlock();
    condition_.notify_all();
    throw SimulationCancelled();
  }

  try {
    std::vector<std::string> next_ids;
    std::vector<bool> next_active;
    std::vector<bool> next_in_flight;
    next_ids.reserve(miner_node_ids_.size() - requested.size());
    next_active.reserve(active_miners_.size() - requested.size());
    next_in_flight.reserve(in_flight_miners_.size() - requested.size());
    for (std::size_t index = 0U; index < miner_node_ids_.size(); ++index) {
      if (requested.contains(miner_node_ids_[index])) {
        continue;
      }
      next_ids.push_back(miner_node_ids_[index]);
      next_active.push_back(active_miners_[index]);
      next_in_flight.push_back(in_flight_miners_[index]);
    }
    return PreparedRemove(this, std::move(lock), std::move(next_ids),
                          std::move(next_active), std::move(next_in_flight));
  } catch (...) {
    membership_mutation_pending_ = false;
    lock.unlock();
    condition_.notify_all();
    throw;
  }
}

void ProbabilisticBlockScheduler::StartMiner(const std::string& node_id) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto miner =
        std::find(miner_node_ids_.begin(), miner_node_ids_.end(), node_id);
    if (miner == miner_node_ids_.end()) {
      throw std::runtime_error("node is not a configured miner: " + node_id);
    }
    const std::size_t index =
        static_cast<std::size_t>(std::distance(miner_node_ids_.begin(), miner));
    active_miners_[index] = true;
  }
  condition_.notify_all();
}

bool ProbabilisticBlockScheduler::StopMiner(const std::string& node_id) {
  std::unique_lock<std::mutex> lock(mutex_);
  const auto miner =
      std::find(miner_node_ids_.begin(), miner_node_ids_.end(), node_id);
  if (miner == miner_node_ids_.end()) {
    throw std::runtime_error("node is not a configured miner: " + node_id);
  }
  const std::size_t index =
      static_cast<std::size_t>(std::distance(miner_node_ids_.begin(), miner));
  const bool was_active = active_miners_[index];
  active_miners_[index] = false;
  condition_.wait(lock, [this, index] { return !in_flight_miners_[index]; });
  return was_active;
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
      condition_.wait(lock, [this] {
        return stop_requested_ || !membership_mutation_pending_;
      });
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
            return stop_requested_ || policy_changed_ ||
                   membership_mutation_pending_;
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
