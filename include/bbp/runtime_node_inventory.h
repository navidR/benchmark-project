#pragma once

#include <compare>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>
#include <string_view>
#include <utility>
#include <vector>

#include "bbp/node_config_snapshot.h"
#include "bbp/simulator/node_runtime.h"

namespace bbp {

struct RuntimeNodeInsertion {
  std::uint32_t slot = 0U;
  std::shared_ptr<NodeRuntime> runtime;
};

class RuntimeNodeSnapshot {
 private:
  struct Generation;
  using EntryIterator = std::vector<RuntimeNodeInsertion>::const_iterator;

 public:
  class Iterator {
   public:
    using iterator_category = std::random_access_iterator_tag;
    using iterator_concept = std::random_access_iterator_tag;
    using value_type = NodeRuntime;
    using difference_type = std::ptrdiff_t;
    using pointer = NodeRuntime*;
    using reference = NodeRuntime&;

    Iterator() = default;

    reference operator*() const;
    pointer operator->() const;
    reference operator[](difference_type offset) const;
    Iterator& operator++();
    Iterator operator++(int);
    Iterator& operator--();
    Iterator operator--(int);
    Iterator& operator+=(difference_type offset);
    Iterator& operator-=(difference_type offset);

    friend Iterator operator+(Iterator iterator, difference_type offset) {
      iterator += offset;
      return iterator;
    }
    friend Iterator operator+(difference_type offset, Iterator iterator) {
      iterator += offset;
      return iterator;
    }
    friend Iterator operator-(Iterator iterator, difference_type offset) {
      iterator -= offset;
      return iterator;
    }
    friend difference_type operator-(const Iterator& left,
                                     const Iterator& right) {
      return left.iterator_ - right.iterator_;
    }
    friend bool operator==(const Iterator&, const Iterator&) = default;
    friend auto operator<=>(const Iterator&, const Iterator&) = default;

   private:
    friend class RuntimeNodeSnapshot;

    explicit Iterator(EntryIterator iterator) : iterator_(iterator) {}

    EntryIterator iterator_;
  };

  RuntimeNodeSnapshot() = default;

  [[nodiscard]] bool empty() const;
  [[nodiscard]] std::size_t size() const;
  [[nodiscard]] std::uint64_t generation() const;
  [[nodiscard]] std::uint32_t slot(std::size_t index) const;
  [[nodiscard]] NodeRuntime& operator[](std::size_t index) const;
  [[nodiscard]] NodeRuntime& at(std::size_t index) const;
  [[nodiscard]] NodeRuntime& front() const;
  [[nodiscard]] NodeRuntime& back() const;
  [[nodiscard]] Iterator begin() const;
  [[nodiscard]] Iterator end() const;

 private:
  friend class RuntimeNodeInventory;

  explicit RuntimeNodeSnapshot(std::shared_ptr<const Generation> generation)
      : generation_(std::move(generation)) {}

  std::shared_ptr<const Generation> generation_;
};

// Run-local immutable-generation inventory. NodeRuntime objects never move
// after publication; each snapshot leases its complete generation.
class RuntimeNodeInventory {
 public:
  class PreparedAppend {
   public:
    PreparedAppend(PreparedAppend&&) noexcept = default;
    PreparedAppend& operator=(PreparedAppend&&) noexcept = default;

    PreparedAppend(const PreparedAppend&) = delete;
    PreparedAppend& operator=(const PreparedAppend&) = delete;

    [[nodiscard]] RuntimeNodeSnapshot Commit() noexcept;

   private:
    friend class RuntimeNodeInventory;

    PreparedAppend(
        RuntimeNodeInventory* owner, std::unique_lock<std::mutex> lock,
        std::shared_ptr<const RuntimeNodeSnapshot::Generation> generation)
        : owner_(owner),
          lock_(std::move(lock)),
          generation_(std::move(generation)) {}

    RuntimeNodeInventory* owner_ = nullptr;
    std::unique_lock<std::mutex> lock_;
    std::shared_ptr<const RuntimeNodeSnapshot::Generation> generation_;
  };

  explicit RuntimeNodeInventory(std::uint32_t capacity);

  RuntimeNodeInventory(const RuntimeNodeInventory&) = delete;
  RuntimeNodeInventory& operator=(const RuntimeNodeInventory&) = delete;

  void Initialize(std::vector<NodeRuntime>& nodes);
  [[nodiscard]] RuntimeNodeSnapshot Snapshot() const;
  [[nodiscard]] NodeConfigSnapshot ConfigSnapshot() const;
  [[nodiscard]] std::uint32_t capacity() const { return capacity_; }

  RuntimeNodeSnapshot PublishAppend(
      std::uint64_t expected_generation,
      const std::vector<RuntimeNodeInsertion>& insertions);
  PreparedAppend PrepareAppend(
      std::uint64_t expected_generation,
      const std::vector<RuntimeNodeInsertion>& insertions);
  PreparedAppend PrepareAppend(
      std::uint64_t expected_generation,
      const std::vector<RuntimeNodeInsertion>& insertions,
      const std::vector<ChainNodeConfig>& published_configs);

 private:
  static std::shared_ptr<RuntimeNodeSnapshot::Generation> MakeGeneration(
      std::uint64_t generation, std::vector<RuntimeNodeInsertion> nodes,
      std::uint32_t capacity);

  const std::uint32_t capacity_;
  mutable std::mutex mutex_;
  std::shared_ptr<const RuntimeNodeSnapshot::Generation> generation_;
};

}  // namespace bbp
