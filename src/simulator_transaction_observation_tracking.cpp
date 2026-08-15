#include "simulator_transaction_observation_tracking.h"

#include <limits>
#include <stdexcept>
#include <utility>

#include "bbp/drivers/chain_driver.h"
#include "bbp/logging.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/options.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_workload_event_details.h"

namespace bbp::simulator_app_internal {

void WriteTransactionLoadProgress(const Options& options,
                                  const std::filesystem::path& events_path,
                                  std::uint32_t workload_index,
                                  std::uint32_t workload_count,
                                  const TransactionLoadSnapshot& snapshot) {
  WriteEvent(
      events_path, options.run_id, "sim",
      SimulationEventKind::kTransactionLoadProgress,
      TransactionLoadProgressDetail(workload_index, workload_count, snapshot));
}

std::optional<TransactionObservationTracker::Reservation>
TransactionObservationTracker::TryReserve(const RuntimeNodeSnapshot& nodes) {
  std::vector<std::string> required_node_ids = ObservableNodeIds(nodes);
  std::optional<TransactionObservationStore::Reservation> observation =
      observations_.TryReserve();
  if (!observation) {
    return std::nullopt;
  }
  return Reservation{
      .observation = std::move(*observation),
      .required_node_ids = std::move(required_node_ids),
  };
}

TransactionObservationTracker::Reservation
TransactionObservationTracker::Reserve(const RuntimeNodeSnapshot& nodes) {
  std::vector<std::string> required_node_ids = ObservableNodeIds(nodes);
  return Reservation{
      .observation = observations_.Reserve(),
      .required_node_ids = std::move(required_node_ids),
  };
}

void TransactionObservationTracker::Track(Reservation reservation,
                                          TrackedTransaction transaction) {
  std::vector<TrackedTransaction> transactions;
  transactions.push_back(std::move(transaction));
  reservation.observation.Commit(std::move(transactions),
                                 reservation.required_node_ids);
}

void TransactionObservationTracker::TrackSet(
    Reservation reservation, std::vector<TrackedTransaction> transactions) {
  reservation.observation.Commit(std::move(transactions),
                                 reservation.required_node_ids);
}

std::size_t TransactionObservationTracker::CancelWorkload(
    std::string_view workload_id) {
  return observations_.CancelWorkload(workload_id);
}

bool TransactionObservationTracker::HasPending() const {
  return !observations_.PendingTransactions().empty();
}

void TransactionObservationTracker::TrackAndWaitForVisibility(
    Reservation reservation, const Options& options,
    const std::filesystem::path& events_path, const ChainDriver& driver,
    const RuntimeNodeSnapshot& nodes, TrackedTransaction transaction,
    std::chrono::seconds timeout, std::stop_token stop_token) {
  const TrackedTransaction tracked = transaction;
  Track(std::move(reservation), std::move(transaction));
  WaitForVisibility(options, events_path, driver, nodes, tracked, timeout,
                    stop_token);
}

void TransactionObservationTracker::WaitForVisibility(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, const RuntimeNodeSnapshot& nodes,
    const TrackedTransaction& tracked, std::chrono::seconds timeout,
    std::stop_token stop_token) {
  for (std::size_t node_index = 0; node_index < nodes.size(); ++node_index) {
    const NodeRuntime& node = nodes[node_index];
    if (!node.AllowsChainMetrics()) {
      continue;
    }
    const ChainTransactionObservation observation = driver.WaitForTransaction(
        node.config, tracked.txid, timeout, stop_token);
    RecordObservation(options, events_path, tracked,
                      static_cast<std::uint32_t>(node_index + 1U),
                      node.config.id, observation);
  }
}

void TransactionObservationTracker::ObserveAll(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, const RuntimeNodeSnapshot& nodes,
    std::stop_token stop_token) {
  const std::vector<TrackedTransaction> transactions =
      observations_.PendingTransactions();
  for (const TrackedTransaction& transaction : transactions) {
    for (std::size_t node_index = 0; node_index < nodes.size(); ++node_index) {
      const NodeRuntime& node = nodes[node_index];
      if (!node.AllowsChainMetrics()) {
        continue;
      }
      const ChainTransactionObservation observation =
          driver.ObserveTransaction(node.config, transaction.txid, stop_token);
      RecordObservation(options, events_path, transaction,
                        static_cast<std::uint32_t>(node_index + 1U),
                        node.config.id, observation);
    }
  }
}

std::vector<TransactionSetObservation>
TransactionObservationTracker::ObserveTrackedSetsUntilVisible(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, const RuntimeNodeSnapshot& nodes,
    const std::vector<std::vector<TrackedTransaction>>& transaction_sets,
    std::chrono::seconds timeout, std::stop_token stop_token) {
  std::vector<TransactionSetObservation> results(transaction_sets.size());
  if (transaction_sets.empty()) {
    return results;
  }
  std::size_t observable_node_count = 0U;
  for (const NodeRuntime& node : nodes) {
    if (node.AllowsChainMetrics()) {
      ++observable_node_count;
    }
  }
  if (observable_node_count == 0U) {
    throw std::runtime_error(
        "transaction load observation requires an observable node");
  }
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  try {
    while (true) {
      ThrowIfStopRequested(stop_token);
      bool all_propagated = true;
      for (std::size_t set_index = 0U; set_index < transaction_sets.size();
           ++set_index) {
        TransactionSetObservation& result = results[set_index];
        if (result.propagated) {
          continue;
        }
        const std::vector<TrackedTransaction>& transaction_set =
            transaction_sets[set_index];
        if (transaction_set.empty()) {
          result.observation_error = true;
          all_propagated = false;
          continue;
        }
        bool set_visible = true;
        bool set_confirmed = true;
        for (const TrackedTransaction& transaction : transaction_set) {
          for (std::size_t node_index = 0U; node_index < nodes.size();
               ++node_index) {
            const NodeRuntime& node = nodes[node_index];
            if (!node.AllowsChainMetrics()) {
              continue;
            }
            try {
              const ChainTransactionObservation observation =
                  driver.ObserveTransactionUntil(node.config, transaction.txid,
                                                 deadline, stop_token);
              RecordObservation(options, events_path, transaction,
                                static_cast<std::uint32_t>(node_index + 1U),
                                node.config.id, observation);
              if (observation.state == ChainTransactionState::kUnknown) {
                set_visible = false;
                set_confirmed = false;
              } else if (observation.state !=
                         ChainTransactionState::kConfirmed) {
                set_confirmed = false;
              }
            } catch (const SimulationCancelled&) {
              throw;
            } catch (const std::exception& error) {
              result.observation_error = true;
              set_visible = false;
              set_confirmed = false;
              BBP_LOG(warning) << "transaction load observation failed for "
                               << transaction.txid << " on " << node.config.id
                               << ": " << error.what();
            }
          }
        }
        result.propagated = set_visible;
        result.confirmed = set_visible && set_confirmed;
        all_propagated = all_propagated && result.propagated;
      }
      if (all_propagated || std::chrono::steady_clock::now() >= deadline) {
        return results;
      }
      WaitForDuration(std::chrono::milliseconds(50), stop_token);
    }
  } catch (...) {
    throw;
  }
}

std::vector<std::string> TransactionObservationTracker::ObservableNodeIds(
    const RuntimeNodeSnapshot& nodes) {
  std::vector<std::string> node_ids;
  node_ids.reserve(nodes.size());
  for (const NodeRuntime& node : nodes) {
    if (node.AllowsChainMetrics()) {
      node_ids.push_back(node.config.id);
    }
  }
  if (node_ids.empty()) {
    throw std::runtime_error(
        "transaction observation requires an observable node");
  }
  return node_ids;
}

void TransactionObservationTracker::RecordObservation(
    const Options& options, const std::filesystem::path& events_path,
    const TrackedTransaction& transaction, std::uint32_t node,
    const std::string& node_id,
    const ChainTransactionObservation& observation) {
  if (observation.state == ChainTransactionState::kUnknown) {
    return;
  }
  if (observation.state == ChainTransactionState::kConfirmed &&
      (!observation.confirmation_height || observation.block_hash.empty() ||
       observation.confirmations == 0U)) {
    throw std::runtime_error(
        "confirmed transaction observation is missing block metadata");
  }
  const std::string detail =
      TransactionObservationDetail(transaction, node, node_id, observation);
  const TransactionObservationTransition transition = observations_.Record(
      transaction.txid, node_id, true,
      observation.state == ChainTransactionState::kConfirmed);
  if (!transition.tracked) {
    return;
  }
  if (transition.first_visible) {
    WriteEvent(events_path, options.run_id, node_id,
               SimulationEventKind::kTransactionVisible, detail);
  }
  if (transition.first_confirmed) {
    WriteEvent(events_path, options.run_id, node_id,
               SimulationEventKind::kTransactionConfirmed, detail);
  }
  if (transition.load_progress) {
    WriteTransactionLoadProgress(
        options, events_path, transaction.workload_index,
        transaction.workload_count, *transition.load_progress);
  }
}

const std::string& RequireSingleWalletTransactionId(
    const ChainWalletTransactionResult& transaction, std::string_view source) {
  if (transaction.txids.size() != 1U || transaction.txids.front().empty()) {
    throw std::runtime_error(std::string(source) +
                             " must return exactly one transaction id");
  }
  return transaction.txids.front();
}

std::vector<TransactionLoadConfirmation::ObservationKey>
ExpectedTransactionLoadObservations(const std::vector<std::string>& txids,
                                    const RuntimeNodeSnapshot& nodes) {
  std::size_t observable_node_count = 0U;
  for (const NodeRuntime& node : nodes) {
    if (node.AllowsChainMetrics()) {
      ++observable_node_count;
    }
  }
  if (observable_node_count == 0U) {
    throw std::runtime_error(
        "transaction load confirmation requires an observable node");
  }
  if (txids.size() >
      std::numeric_limits<std::size_t>::max() / observable_node_count) {
    throw std::runtime_error(
        "transaction load confirmation observation count overflows size_t");
  }
  std::vector<TransactionLoadConfirmation::ObservationKey> expected;
  expected.reserve(txids.size() * observable_node_count);
  for (const std::string& txid : txids) {
    for (const NodeRuntime& node : nodes) {
      if (node.AllowsChainMetrics()) {
        expected.emplace_back(txid, node.config.id);
      }
    }
  }
  return expected;
}

}  // namespace bbp::simulator_app_internal
