#include "bbp/runtime_node_inventory.h"

#include <algorithm>
#include <exception>
#include <limits>
#include <set>
#include <stdexcept>
#include <string>

namespace bbp {

struct RuntimeNodeSnapshot::Generation {
  std::uint64_t sequence = 0U;
  std::vector<RuntimeNodeInsertion> nodes;
  std::vector<ChainNodeConfig> configs;
};

RuntimeNodeSnapshot::Iterator::reference
RuntimeNodeSnapshot::Iterator::operator*() const {
  return *iterator_->runtime;
}

RuntimeNodeSnapshot::Iterator::pointer
RuntimeNodeSnapshot::Iterator::operator->() const {
  return iterator_->runtime.get();
}

RuntimeNodeSnapshot::Iterator::reference
RuntimeNodeSnapshot::Iterator::operator[](difference_type offset) const {
  return *iterator_[offset].runtime;
}

RuntimeNodeSnapshot::Iterator& RuntimeNodeSnapshot::Iterator::operator++() {
  ++iterator_;
  return *this;
}

RuntimeNodeSnapshot::Iterator RuntimeNodeSnapshot::Iterator::operator++(int) {
  Iterator copy = *this;
  ++*this;
  return copy;
}

RuntimeNodeSnapshot::Iterator& RuntimeNodeSnapshot::Iterator::operator--() {
  --iterator_;
  return *this;
}

RuntimeNodeSnapshot::Iterator RuntimeNodeSnapshot::Iterator::operator--(int) {
  Iterator copy = *this;
  --*this;
  return copy;
}

RuntimeNodeSnapshot::Iterator& RuntimeNodeSnapshot::Iterator::operator+=(
    difference_type offset) {
  iterator_ += offset;
  return *this;
}

RuntimeNodeSnapshot::Iterator& RuntimeNodeSnapshot::Iterator::operator-=(
    difference_type offset) {
  iterator_ -= offset;
  return *this;
}

bool RuntimeNodeSnapshot::empty() const { return size() == 0U; }

std::size_t RuntimeNodeSnapshot::size() const {
  return generation_ ? generation_->nodes.size() : 0U;
}

std::uint64_t RuntimeNodeSnapshot::generation() const {
  return generation_ ? generation_->sequence : 0U;
}

std::uint32_t RuntimeNodeSnapshot::slot(std::size_t index) const {
  if (!generation_ || index >= generation_->nodes.size()) {
    throw std::out_of_range("runtime node snapshot slot is out of range");
  }
  return generation_->nodes[index].slot;
}

NodeRuntime& RuntimeNodeSnapshot::operator[](std::size_t index) const {
  if (!generation_ || index >= generation_->nodes.size()) {
    throw std::out_of_range("runtime node snapshot index is out of range");
  }
  return *generation_->nodes[index].runtime;
}

NodeRuntime& RuntimeNodeSnapshot::at(std::size_t index) const {
  return operator[](index);
}

NodeRuntime& RuntimeNodeSnapshot::front() const {
  if (empty()) {
    throw std::out_of_range("runtime node snapshot is empty");
  }
  return operator[](0U);
}

NodeRuntime& RuntimeNodeSnapshot::back() const {
  if (empty()) {
    throw std::out_of_range("runtime node snapshot is empty");
  }
  return operator[](size() - 1U);
}

RuntimeNodeSnapshot::Iterator RuntimeNodeSnapshot::begin() const {
  return Iterator(generation_ ? generation_->nodes.begin() : EntryIterator{});
}

RuntimeNodeSnapshot::Iterator RuntimeNodeSnapshot::end() const {
  return Iterator(generation_ ? generation_->nodes.end() : EntryIterator{});
}

RuntimeNodeInventory::RuntimeNodeInventory(std::uint32_t capacity)
    : capacity_(capacity), generation_(MakeGeneration(0U, {}, capacity)) {
  if (capacity_ == 0U) {
    throw std::invalid_argument("runtime node capacity must be positive");
  }
}

void RuntimeNodeInventory::Initialize(std::vector<NodeRuntime>& nodes) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (generation_->sequence != 0U || !generation_->nodes.empty()) {
    throw std::logic_error("runtime node inventory is already initialized");
  }
  if (nodes.size() > capacity_) {
    throw std::invalid_argument(
        "initial runtime node count exceeds configured capacity");
  }
  std::set<std::string> node_ids;
  std::vector<ChainNodeConfig> configs;
  configs.reserve(nodes.size());
  for (const NodeRuntime& node : nodes) {
    if (node.config.id.empty() || !node_ids.insert(node.config.id).second) {
      throw std::invalid_argument(
          "initial runtime nodes have an empty or duplicate node id");
    }
    configs.push_back(node.config);
  }
  auto owner = std::make_shared<std::vector<NodeRuntime>>();
  auto generation = std::make_shared<RuntimeNodeSnapshot::Generation>();
  std::vector<RuntimeNodeInsertion> insertions;
  insertions.reserve(nodes.size());
  *owner = std::move(nodes);
  for (std::size_t index = 0U; index < owner->size(); ++index) {
    if (index > std::numeric_limits<std::uint32_t>::max()) {
      throw std::overflow_error("runtime node slot exceeds uint32");
    }
    insertions.push_back(RuntimeNodeInsertion{
        .slot = static_cast<std::uint32_t>(index),
        .runtime = std::shared_ptr<NodeRuntime>(owner, &(*owner)[index]),
    });
  }
  generation->sequence = 1U;
  generation->nodes = std::move(insertions);
  generation->configs = std::move(configs);
  generation_ = std::move(generation);
}

RuntimeNodeSnapshot RuntimeNodeInventory::Snapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return RuntimeNodeSnapshot(generation_);
}

NodeConfigSnapshot RuntimeNodeInventory::ConfigSnapshot() const {
  std::shared_ptr<const RuntimeNodeSnapshot::Generation> generation;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    generation = generation_;
  }
  return NodeConfigSnapshot(generation->configs, generation);
}

RuntimeNodeSnapshot RuntimeNodeInventory::PublishAppend(
    std::uint64_t expected_generation,
    const std::vector<RuntimeNodeInsertion>& insertions) {
  PreparedAppend prepared = PrepareAppend(expected_generation, insertions);
  return prepared.Commit();
}

RuntimeNodeInventory::PreparedAppend RuntimeNodeInventory::PrepareAppend(
    std::uint64_t expected_generation,
    const std::vector<RuntimeNodeInsertion>& insertions) {
  return PrepareAppend(expected_generation, insertions, {});
}

RuntimeNodeInventory::PreparedAppend RuntimeNodeInventory::PrepareAppend(
    std::uint64_t expected_generation,
    const std::vector<RuntimeNodeInsertion>& insertions,
    const std::vector<ChainNodeConfig>& published_configs) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (generation_->sequence != expected_generation) {
    throw std::runtime_error(
        "runtime node inventory changed before publication");
  }
  if (insertions.empty()) {
    throw std::invalid_argument(
        "runtime node publication requires at least one insertion");
  }
  if (insertions.size() > capacity_ - generation_->nodes.size()) {
    throw std::runtime_error(
        "runtime node publication exceeds configured capacity");
  }
  if (generation_->sequence == std::numeric_limits<std::uint64_t>::max()) {
    throw std::overflow_error("runtime node inventory generation overflow");
  }
  std::vector<RuntimeNodeInsertion> next = generation_->nodes;
  next.reserve(next.size() + insertions.size());
  next.insert(next.end(), insertions.begin(), insertions.end());
  auto next_generation =
      MakeGeneration(generation_->sequence + 1U, std::move(next), capacity_);
  if (!published_configs.empty()) {
    if (published_configs.size() != next_generation->nodes.size()) {
      throw std::invalid_argument(
          "runtime node published configs must match the next generation");
    }
    for (std::size_t index = 0U; index < published_configs.size(); ++index) {
      if (published_configs[index].id !=
          next_generation->nodes[index].runtime->config.id) {
        throw std::invalid_argument(
            "runtime node published config identity does not match its "
            "runtime");
      }
    }
    next_generation->configs = published_configs;
  }
  return PreparedAppend(this, std::move(lock), std::move(next_generation));
}

RuntimeNodeSnapshot RuntimeNodeInventory::PreparedAppend::Commit() noexcept {
  if (owner_ == nullptr || !lock_.owns_lock() ||
      lock_.mutex() != &owner_->mutex_ || !generation_) {
    std::terminate();
  }
  owner_->generation_.swap(generation_);
  RuntimeNodeSnapshot snapshot(owner_->generation_);
  lock_.unlock();
  owner_ = nullptr;
  generation_.reset();
  return snapshot;
}

std::shared_ptr<RuntimeNodeSnapshot::Generation>
RuntimeNodeInventory::MakeGeneration(std::uint64_t generation,
                                     std::vector<RuntimeNodeInsertion> nodes,
                                     std::uint32_t capacity) {
  std::set<std::uint32_t> slots;
  std::set<std::string> node_ids;
  std::vector<ChainNodeConfig> configs;
  configs.reserve(nodes.size());
  for (const RuntimeNodeInsertion& insertion : nodes) {
    if (!insertion.runtime) {
      throw std::invalid_argument("runtime node insertion is empty");
    }
    if (insertion.runtime->config.id.empty() ||
        !node_ids.insert(insertion.runtime->config.id).second) {
      throw std::invalid_argument(
          "runtime node generation has an empty or duplicate node id");
    }
    if (!slots.insert(insertion.slot).second) {
      throw std::invalid_argument(
          "runtime node generation has a duplicate resource slot");
    }
    if (insertion.slot >= capacity) {
      throw std::invalid_argument(
          "runtime node generation has an out-of-range resource slot");
    }
    configs.push_back(insertion.runtime->config);
  }
  return std::make_shared<RuntimeNodeSnapshot::Generation>(
      RuntimeNodeSnapshot::Generation{
          .sequence = generation,
          .nodes = std::move(nodes),
          .configs = std::move(configs),
      });
}

}  // namespace bbp
