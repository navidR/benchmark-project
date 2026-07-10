#include "benchmark_sim/simulation_command_queue.h"

#include <stdexcept>
#include <utility>

namespace bsim {

std::uint64_t SimulationCommandQueue::Push(SimulationCommandKind kind,
                                           std::string node_id) {
  if (node_id.empty()) {
    throw std::runtime_error("simulation command requires a node id");
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (closed_) {
    throw std::runtime_error("simulation command queue is closed");
  }
  const std::uint64_t sequence = next_sequence_++;
  commands_.push_back(SimulationCommand{
      .sequence = sequence,
      .kind = kind,
      .node_id = std::move(node_id),
  });
  ready_.notify_one();
  return sequence;
}

std::optional<SimulationCommand> SimulationCommandQueue::TryPop() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (commands_.empty()) {
    return std::nullopt;
  }
  SimulationCommand command = std::move(commands_.front());
  commands_.pop_front();
  return command;
}

std::optional<SimulationCommand> SimulationCommandQueue::WaitPop() {
  std::unique_lock<std::mutex> lock(mutex_);
  ready_.wait(lock, [this] { return closed_ || !commands_.empty(); });
  if (commands_.empty()) {
    return std::nullopt;
  }
  SimulationCommand command = std::move(commands_.front());
  commands_.pop_front();
  return command;
}

void SimulationCommandQueue::Close() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
  }
  ready_.notify_all();
}

bool SimulationCommandQueue::IsClosed() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return closed_;
}

}  // namespace bsim
