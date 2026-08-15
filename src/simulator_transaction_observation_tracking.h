#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/simulator/transaction_observation_store.h"

namespace bbp {

class ChainDriver;
class RuntimeNodeSnapshot;
struct ChainTransactionObservation;
struct ChainWalletTransactionResult;
struct Options;

namespace simulator_app_internal {

void WriteTransactionLoadProgress(const Options& options,
                                  const std::filesystem::path& events_path,
                                  std::uint32_t workload_index,
                                  std::uint32_t workload_count,
                                  const TransactionLoadSnapshot& snapshot);

struct TransactionSetObservation {
  bool propagated = false;
  bool confirmed = false;
  bool observation_error = false;
};

class TransactionObservationTracker {
 public:
  struct Reservation {
    TransactionObservationStore::Reservation observation;
    std::vector<std::string> required_node_ids;
  };

  [[nodiscard]] std::optional<Reservation> TryReserve(
      const RuntimeNodeSnapshot& nodes);
  [[nodiscard]] Reservation Reserve(const RuntimeNodeSnapshot& nodes);
  void Track(Reservation reservation, TrackedTransaction transaction);
  void TrackSet(Reservation reservation,
                std::vector<TrackedTransaction> transactions);
  std::size_t CancelWorkload(std::string_view workload_id);
  [[nodiscard]] bool HasPending() const;
  void TrackAndWaitForVisibility(
      Reservation reservation, const Options& options,
      const std::filesystem::path& events_path, const ChainDriver& driver,
      const RuntimeNodeSnapshot& nodes, TrackedTransaction transaction,
      std::chrono::seconds timeout, std::stop_token stop_token);
  void WaitForVisibility(const Options& options,
                         const std::filesystem::path& events_path,
                         const ChainDriver& driver,
                         const RuntimeNodeSnapshot& nodes,
                         const TrackedTransaction& tracked,
                         std::chrono::seconds timeout,
                         std::stop_token stop_token);
  void ObserveAll(const Options& options,
                  const std::filesystem::path& events_path,
                  const ChainDriver& driver, const RuntimeNodeSnapshot& nodes,
                  std::stop_token stop_token);
  std::vector<TransactionSetObservation> ObserveTrackedSetsUntilVisible(
      const Options& options, const std::filesystem::path& events_path,
      const ChainDriver& driver, const RuntimeNodeSnapshot& nodes,
      const std::vector<std::vector<TrackedTransaction>>& transaction_sets,
      std::chrono::seconds timeout, std::stop_token stop_token);

 private:
  static std::vector<std::string> ObservableNodeIds(
      const RuntimeNodeSnapshot& nodes);
  void RecordObservation(const Options& options,
                         const std::filesystem::path& events_path,
                         const TrackedTransaction& transaction,
                         std::uint32_t node, const std::string& node_id,
                         const ChainTransactionObservation& observation);

  TransactionObservationStore observations_;
};

const std::string& RequireSingleWalletTransactionId(
    const ChainWalletTransactionResult& transaction, std::string_view source);
std::vector<TransactionLoadConfirmation::ObservationKey>
ExpectedTransactionLoadObservations(const std::vector<std::string>& txids,
                                    const RuntimeNodeSnapshot& nodes);

}  // namespace simulator_app_internal
}  // namespace bbp
