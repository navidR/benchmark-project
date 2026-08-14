#include "simulator_workload_event_details.h"

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <utility>

#include "bbp/drivers/chain_driver.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulation_wallet_send.h"
#include "bbp/simulator/send_raw_transaction_workload.h"
#include "bbp/simulator/transaction_load.h"
#include "bbp/simulator/transaction_observation_store.h"
#include "bbp/simulator/wallet_transactions_workload.h"
#include "bbp/util.h"

namespace bbp::simulator_app_internal {

std::string GeneratedBlocksDetail(
    uint32_t workload_index, uint32_t workload_count, uint32_t generator_node,
    uint64_t start_height, uint64_t target_height,
    const std::vector<std::string>& hashes, const std::string& reward_address,
    std::optional<std::uint64_t> operator_command_sequence,
    std::optional<std::string_view> workload_id) {
  boost::json::array hash_array;
  for (const std::string& hash : hashes) {
    hash_array.emplace_back(hash);
  }
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["generator_node"] = generator_node;
  detail["count"] = static_cast<uint64_t>(hashes.size());
  detail["start_height"] = start_height;
  detail["target_height"] = target_height;
  detail["reward_address"] = reward_address;
  detail["hashes"] = std::move(hash_array);
  if (operator_command_sequence) {
    detail["operator_command_sequence"] = *operator_command_sequence;
  }
  if (workload_id) {
    detail["workload_id"] = *workload_id;
  }
  return boost::json::serialize(detail);
}

std::string HeightWaitDetail(uint32_t workload_index, uint32_t workload_count,
                             uint32_t node, uint64_t target_height,
                             uint64_t observed_height,
                             std::optional<std::string_view> workload_id) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = node;
  detail["target_height"] = target_height;
  detail["observed_height"] = observed_height;
  if (workload_id) {
    detail["workload_id"] = *workload_id;
  }
  return boost::json::serialize(detail);
}

std::string PeerCountWaitDetail(uint32_t workload_index,
                                uint32_t workload_count, uint32_t node,
                                uint64_t target_peer_count,
                                uint64_t observed_peer_count,
                                std::optional<std::string_view> workload_id) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = node;
  detail["target_peer_count"] = target_peer_count;
  detail["observed_peer_count"] = observed_peer_count;
  if (workload_id) {
    detail["workload_id"] = *workload_id;
  }
  return boost::json::serialize(detail);
}

std::string PeerChurnDetail(uint32_t workload_index, uint32_t workload_count,
                            uint32_t node, uint32_t peer,
                            const std::string& address,
                            uint64_t before_peer_count,
                            uint64_t after_peer_count, bool connected_before,
                            bool connected_after,
                            std::optional<uint32_t> timeout_sec) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = node;
  detail["peer"] = peer;
  detail["address"] = address;
  detail["before_peer_count"] = before_peer_count;
  detail["after_peer_count"] = after_peer_count;
  detail["connected_before"] = connected_before;
  detail["connected_after"] = connected_after;
  if (timeout_sec) {
    detail["timeout_sec"] = *timeout_sec;
  }
  return boost::json::serialize(detail);
}

std::string RawTransactionDetail(uint32_t workload_index,
                                 uint32_t workload_count,
                                 const SendRawTransactionWorkload& workload,
                                 uint64_t start_height, uint64_t target_height,
                                 const std::vector<std::string>& funding_hashes,
                                 const ChainRawTransactionResult& transaction,
                                 bool mempool_size_observed) {
  boost::json::object utxo;
  utxo["txid"] = transaction.utxo.txid;
  utxo["vout"] = transaction.utxo.vout;
  utxo["amount"] = transaction.utxo.amount;
  utxo["block_hash"] = transaction.utxo.block_hash;
  utxo["confirmations"] = transaction.utxo.confirmations;

  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["funding_node"] = workload.funding_node;
  detail["submit_node"] = workload.submit_node;
  detail["source_address"] = workload.source_address;
  detail["destination_address"] = workload.destination_address;
  detail["funding_blocks"] = workload.funding_blocks;
  detail["funding_hash_count"] = static_cast<uint64_t>(funding_hashes.size());
  detail["funding_start_height"] = start_height;
  detail["funding_target_height"] = target_height;
  detail["selected_utxo"] = std::move(utxo);
  detail["amount"] = transaction.destination_amount;
  detail["fee"] = transaction.fee;
  detail["change_amount"] = transaction.change_amount;
  detail["txid"] = transaction.txid;
  if (mempool_size_observed) {
    detail["mempool_size"] = transaction.mempool_size;
  } else {
    detail["mempool_size"] = nullptr;
  }
  detail["timeout_sec"] = workload.timeout_sec;
  return boost::json::serialize(detail);
}

namespace {

boost::json::array TxIdsJson(const std::vector<std::string>& txids) {
  boost::json::array array;
  for (const std::string& txid : txids) {
    array.emplace_back(txid);
  }
  return array;
}

boost::json::object AmountDistributionDetail(
    const AmountDistribution& distribution) {
  boost::json::object object;
  object["distribution"] =
      std::string(ValueDistributionKindName(distribution.kind));
  object["min"] = FormatFixed8Amount(distribution.minimum_satoshis);
  object["max"] = FormatFixed8Amount(distribution.maximum_satoshis);
  object["min_satoshis"] = distribution.minimum_satoshis;
  object["max_satoshis"] = distribution.maximum_satoshis;
  return object;
}

boost::json::object IntervalDistributionDetail(
    const IntervalDistribution& distribution) {
  boost::json::object object;
  object["distribution"] =
      std::string(ValueDistributionKindName(distribution.kind));
  object["min_ms"] = distribution.minimum.count();
  object["max_ms"] = distribution.maximum.count();
  return object;
}

}  // namespace

std::string WalletFundingDetail(
    uint32_t workload_index, uint32_t workload_count,
    const WalletTransactionsWorkload& workload, const WalletIdentity& wallet,
    uint32_t miner_node, uint64_t start_height, uint64_t target_height,
    uint64_t funding_hash_count, uint64_t ready_height,
    const ChainWalletFundingResult& preparation,
    uint64_t preparation_hash_count, uint64_t ready_balance_satoshis) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["wallet_index"] = wallet.wallet_index;
  detail["node"] = wallet.node;
  if (!wallet.node_id.empty()) {
    detail["node_id"] = wallet.node_id;
  }
  detail["address"] = wallet.address;
  detail["funding_address"] = wallet.funding_address;
  detail["miner_node"] = miner_node;
  detail["funding_strategy"] =
      std::string(WalletFundingStrategyName(workload.funding_strategy));
  detail["seed"] = workload.random_seed;
  detail["funding_blocks_per_wallet"] = workload.funding_blocks_per_wallet;
  detail["funding_hash_count"] = funding_hash_count;
  detail["funding_start_height"] = start_height;
  detail["funding_target_height"] = target_height;
  detail["funding_ready_height"] = ready_height;
  detail["funding_preparation_txids"] = TxIdsJson(preparation.txids);
  detail["funding_preparation_confirmation_blocks"] =
      preparation.confirmation_blocks_required;
  detail["funding_preparation_minimum_chain_height"] =
      preparation.minimum_chain_height;
  detail["funding_preparation_hash_count"] = preparation_hash_count;
  detail["readiness_confirmations"] = workload.readiness_confirmations;
  detail["funding_threshold"] =
      FormatFixed8Amount(workload.funding_threshold_satoshis);
  detail["funding_threshold_satoshis"] = workload.funding_threshold_satoshis;
  detail["ready_balance"] = FormatFixed8Amount(ready_balance_satoshis);
  detail["ready_balance_satoshis"] = ready_balance_satoshis;
  detail["amount_distribution"] = AmountDistributionDetail(workload.amount);
  detail["interval_distribution"] =
      IntervalDistributionDetail(workload.interval);
  return boost::json::serialize(detail);
}

std::string WalletTransactionDetail(
    uint32_t workload_index, uint32_t workload_count,
    const WalletTransactionsWorkload& workload, uint64_t transaction_index,
    const WalletIdentity& sender, const WalletIdentity& receiver,
    uint32_t funding_miner_node, uint64_t funding_start_height,
    uint64_t funding_target_height, uint64_t funding_hash_count,
    uint64_t funding_ready_height,
    const ChainWalletFundingResult& funding_preparation,
    uint64_t funding_preparation_hash_count,
    uint64_t funding_ready_balance_satoshis, uint64_t amount_satoshis,
    std::chrono::milliseconds interval_before,
    std::optional<std::chrono::milliseconds> scheduled_simulation_elapsed,
    std::optional<std::chrono::milliseconds> scheduled_wall_elapsed,
    const ChainWalletTransactionResult& transaction) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["transaction_index"] = transaction_index;
  if (workload.transaction_count != 0U) {
    detail["transaction_count"] = workload.transaction_count;
  } else {
    detail["transaction_count"] = nullptr;
  }
  if (workload.transaction_rate) {
    detail["transaction_rate"] = workload.transaction_rate->value();
  } else {
    detail["transaction_rate"] = nullptr;
  }
  detail["strategy"] =
      std::string(WalletTransferStrategyName(workload.strategy));
  detail["funding_strategy"] =
      std::string(WalletFundingStrategyName(workload.funding_strategy));
  detail["seed"] = workload.random_seed;
  detail["sender_wallet_index"] = sender.wallet_index;
  detail["receiver_wallet_index"] = receiver.wallet_index;
  detail["sender_node"] = sender.node;
  detail["receiver_node"] = receiver.node;
  detail["sender_address"] = sender.address;
  detail["receiver_address"] = receiver.address;
  detail["funding_miner_node"] = funding_miner_node;
  detail["funding_blocks_per_wallet"] = workload.funding_blocks_per_wallet;
  detail["funding_hash_count"] = funding_hash_count;
  detail["funding_start_height"] = funding_start_height;
  detail["funding_target_height"] = funding_target_height;
  detail["funding_ready_height"] = funding_ready_height;
  detail["funding_preparation_txids"] = TxIdsJson(funding_preparation.txids);
  detail["funding_preparation_confirmation_blocks"] =
      funding_preparation.confirmation_blocks_required;
  detail["funding_preparation_minimum_chain_height"] =
      funding_preparation.minimum_chain_height;
  detail["funding_preparation_hash_count"] = funding_preparation_hash_count;
  detail["readiness_confirmations"] = workload.readiness_confirmations;
  detail["funding_threshold"] =
      FormatFixed8Amount(workload.funding_threshold_satoshis);
  detail["funding_threshold_satoshis"] = workload.funding_threshold_satoshis;
  detail["funding_ready_balance"] =
      FormatFixed8Amount(funding_ready_balance_satoshis);
  detail["funding_ready_balance_satoshis"] = funding_ready_balance_satoshis;
  detail["amount_distribution"] = AmountDistributionDetail(workload.amount);
  detail["interval_distribution"] =
      IntervalDistributionDetail(workload.interval);
  detail["interval_before_ms"] = interval_before.count();
  if (scheduled_simulation_elapsed) {
    detail["scheduled_simulation_elapsed_ms"] =
        scheduled_simulation_elapsed->count();
  } else {
    detail["scheduled_simulation_elapsed_ms"] = nullptr;
  }
  if (scheduled_wall_elapsed) {
    detail["scheduled_wall_elapsed_ms"] = scheduled_wall_elapsed->count();
  } else {
    detail["scheduled_wall_elapsed_ms"] = nullptr;
  }
  detail["amount"] = FormatFixed8Amount(amount_satoshis);
  detail["amount_satoshis"] = amount_satoshis;
  detail["requested_fee_rate"] = transaction.requested_fee_rate;
  detail["requested_fee_rate_satoshis"] = workload.fee_satoshis;
  detail["txids"] = TxIdsJson(transaction.txids);
  detail["mempool_size"] = transaction.mempool_size;
  detail["timeout_sec"] = workload.timeout_sec;
  return boost::json::serialize(detail);
}

std::string TransactionLoadAttemptDetail(
    uint32_t workload_index, uint32_t workload_count,
    const WalletTransactionsWorkload& workload,
    std::optional<std::uint64_t> attempt_limit,
    const WalletTransactionLoadTask& task, const WalletIdentity& sender,
    const WalletIdentity& receiver, TransactionLoadOutcome outcome,
    std::chrono::microseconds latency,
    const ChainWalletTransactionResult* transaction,
    std::string_view error_class, std::string_view error_message) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["transaction_index"] = task.transaction_index;
  if (attempt_limit) {
    detail["attempt_limit"] = *attempt_limit;
  } else {
    detail["attempt_limit"] = nullptr;
  }
  detail["strategy"] =
      std::string(WalletTransferStrategyName(workload.strategy));
  detail["mode"] = std::string(WalletPrivacyModeName(workload.mode));
  detail["seed"] = workload.random_seed;
  detail["sender_wallet_index"] = sender.wallet_index;
  detail["receiver_wallet_index"] = receiver.wallet_index;
  detail["sender_node"] = sender.node;
  detail["receiver_node"] = receiver.node;
  detail["amount"] = FormatFixed8Amount(task.plan.amount_satoshis);
  detail["amount_satoshis"] = task.plan.amount_satoshis;
  detail["fee_policy"] =
      std::string(WalletTransactionFeePolicyName(workload.fee_policy));
  detail["fee"] = FormatFixed8Amount(workload.fee_satoshis);
  detail["fee_satoshis"] = workload.fee_satoshis;
  detail["fee_reserve_satoshis"] =
      EffectiveWalletTransactionFeeReserveSatoshis(workload);
  detail["amount_distribution"] = AmountDistributionDetail(workload.amount);
  if (task.scheduled_simulation_elapsed) {
    detail["scheduled_simulation_elapsed_ms"] =
        task.scheduled_simulation_elapsed->count();
  } else {
    detail["scheduled_simulation_elapsed_ms"] = nullptr;
  }
  if (task.scheduled_wall_elapsed) {
    detail["scheduled_wall_elapsed_ms"] = task.scheduled_wall_elapsed->count();
  } else {
    detail["scheduled_wall_elapsed_ms"] = nullptr;
  }
  detail["outcome"] = std::string(TransactionLoadOutcomeName(outcome));
  detail["backpressured"] = outcome == TransactionLoadOutcome::kBackpressured;
  detail["dropped"] = outcome == TransactionLoadOutcome::kBackpressured ||
                      outcome == TransactionLoadOutcome::kDropped;
  detail["cancelled"] = outcome == TransactionLoadOutcome::kCancelled;
  detail["latency_us"] = static_cast<std::uint64_t>(latency.count());
  detail["latency_ms"] = static_cast<double>(latency.count()) / 1000.0;
  if (transaction != nullptr) {
    detail["txids"] = TxIdsJson(transaction->txids);
    detail["mempool_size"] = transaction->mempool_size;
    detail["requested_fee_rate"] = transaction->requested_fee_rate;
  } else {
    detail["txids"] = boost::json::array{};
    detail["mempool_size"] = nullptr;
    detail["requested_fee_rate"] = nullptr;
  }
  if (error_message.empty()) {
    detail["error"] = nullptr;
  } else {
    detail["error"] = error_message;
  }
  if (error_class.empty()) {
    detail["error_class"] = nullptr;
  } else {
    detail["error_class"] = error_class;
  }
  return boost::json::serialize(detail);
}

namespace {

void AddTransactionLoadCounterFields(const TransactionLoadSnapshot& snapshot,
                                     boost::json::object* detail) {
  (*detail)["revision"] = snapshot.revision;
  (*detail)["attempted"] = snapshot.attempted;
  (*detail)["submitted"] = snapshot.submitted;
  (*detail)["rejected"] = snapshot.rejected;
  (*detail)["timed_out"] = snapshot.timed_out;
  (*detail)["backpressured"] = snapshot.backpressured;
  (*detail)["dropped"] = snapshot.dropped;
  (*detail)["cancelled"] = snapshot.cancelled;
  (*detail)["propagated"] = snapshot.propagated;
  (*detail)["confirmed"] = snapshot.confirmed;
  (*detail)["failed"] = snapshot.failed;
  (*detail)["observation_errors"] = snapshot.observation_errors;
  (*detail)["latency_sample_count"] = snapshot.latency_sample_count;
  (*detail)["latency_total_us"] = snapshot.latency_total_us;
  (*detail)["latency_min_us"] = snapshot.latency_min_us;
  (*detail)["latency_max_us"] = snapshot.latency_max_us;
  (*detail)["accounting_invariants_hold"] = snapshot.InvariantsHold();
}

void AddTransactionLoadSnapshotFields(const TransactionLoadSnapshot& snapshot,
                                      boost::json::object* detail) {
  AddTransactionLoadCounterFields(snapshot, detail);
  (*detail)["average_latency_ms"] = snapshot.average_latency_ms;
  (*detail)["elapsed_us"] = snapshot.elapsed_us;
  (*detail)["attempted_per_second"] = snapshot.attempted_per_second;
  (*detail)["submitted_per_second"] = snapshot.submitted_per_second;
  (*detail)["propagated_per_second"] = snapshot.propagated_per_second;
  (*detail)["confirmed_per_second"] = snapshot.confirmed_per_second;
}

}  // namespace

std::string TransactionLoadProgressDetail(
    std::uint32_t workload_index, std::uint32_t workload_count,
    const TransactionLoadSnapshot& snapshot) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  AddTransactionLoadCounterFields(snapshot, &detail);
  return boost::json::serialize(detail);
}

std::string TransactionLoadCompletedDetail(
    uint32_t workload_index, uint32_t workload_count,
    const WalletTransactionsWorkload& workload,
    std::optional<std::uint64_t> attempt_limit, std::size_t queue_maximum_size,
    const TransactionLoadSnapshot& snapshot) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["strategy"] =
      std::string(WalletTransferStrategyName(workload.strategy));
  detail["mode"] = std::string(WalletPrivacyModeName(workload.mode));
  detail["seed"] = workload.random_seed;
  if (attempt_limit) {
    detail["configured_attempt_limit"] = *attempt_limit;
  } else {
    detail["configured_attempt_limit"] = nullptr;
  }
  if (workload.transaction_rate) {
    detail["configured_rate"] = workload.transaction_rate->value();
  } else {
    detail["configured_rate"] = nullptr;
  }
  if (workload.duration) {
    detail["configured_duration_ms"] = workload.duration->count();
  } else {
    detail["configured_duration_ms"] = nullptr;
  }
  detail["configured_concurrency"] = workload.concurrency;
  detail["queue_capacity"] = workload.queue_capacity;
  detail["queue_maximum_depth"] = queue_maximum_size;
  detail["fee_reserve_satoshis"] =
      EffectiveWalletTransactionFeeReserveSatoshis(workload);
  AddTransactionLoadSnapshotFields(snapshot, &detail);
  return boost::json::serialize(detail);
}

std::string OperatorWalletTransactionDetail(
    const SimulationWalletSend& send, const WalletIdentity& sender,
    const WalletIdentity& receiver,
    const WalletInitialization& wallet_initialization,
    const ChainWalletTransactionResult& transaction,
    std::uint64_t operator_command_sequence) {
  boost::json::object detail;
  detail["workload_index"] = 0U;
  detail["workload_count"] = 0U;
  detail["transaction_index"] = operator_command_sequence;
  detail["transaction_count"] = nullptr;
  detail["transaction_rate"] = nullptr;
  detail["submission_kind"] = "operator_wallet_send";
  detail["operator_command_sequence"] = operator_command_sequence;
  detail["mode"] =
      std::string(WalletPrivacyModeName(wallet_initialization.mode));
  detail["sender_wallet_index"] = sender.wallet_index;
  detail["receiver_wallet_index"] = receiver.wallet_index;
  detail["sender_node"] = sender.node;
  detail["receiver_node"] = receiver.node;
  detail["sender_address"] = sender.address;
  detail["receiver_address"] = receiver.address;
  detail["amount"] = FormatFixed8Amount(send.amount_satoshis);
  detail["amount_satoshis"] = send.amount_satoshis;
  detail["fee"] = FormatFixed8Amount(send.fee_satoshis);
  detail["fee_satoshis"] = send.fee_satoshis;
  detail["requested_fee_rate"] = transaction.requested_fee_rate;
  detail["requested_fee_rate_satoshis"] = send.fee_satoshis;
  detail["txids"] = TxIdsJson(transaction.txids);
  detail["mempool_size"] = transaction.mempool_size;
  detail["timeout_sec"] = send.timeout_sec;
  return boost::json::serialize(detail);
}

std::string TransactionObservationDetail(
    const TrackedTransaction& transaction, std::uint32_t node,
    const std::string& node_id,
    const ChainTransactionObservation& observation) {
  boost::json::object detail;
  detail["txid"] = transaction.txid;
  detail["submission_kind"] = transaction.submission_kind;
  detail["workload_index"] = transaction.workload_index;
  detail["workload_count"] = transaction.workload_count;
  detail["transaction_index"] = transaction.transaction_index;
  if (transaction.transaction_count) {
    detail["transaction_count"] = *transaction.transaction_count;
  } else {
    detail["transaction_count"] = nullptr;
  }
  if (transaction.transaction_rate) {
    detail["transaction_rate"] = *transaction.transaction_rate;
  } else {
    detail["transaction_rate"] = nullptr;
  }
  detail["txid_index"] = transaction.txid_index;
  detail["submission_node"] = transaction.submission_node;
  detail["node"] = node;
  detail["node_id"] = node_id;
  detail["state"] = ChainTransactionStateName(observation.state);
  detail["observed_height"] = observation.observed_height;
  detail["mempool_size"] = observation.mempool_size;
  if (observation.block_hash.empty()) {
    detail["block_hash"] = nullptr;
  } else {
    detail["block_hash"] = observation.block_hash;
  }
  if (observation.confirmation_height) {
    detail["confirmation_height"] = *observation.confirmation_height;
  } else {
    detail["confirmation_height"] = nullptr;
  }
  detail["confirmations"] = observation.confirmations;
  return boost::json::serialize(detail);
}

}  // namespace bbp::simulator_app_internal
