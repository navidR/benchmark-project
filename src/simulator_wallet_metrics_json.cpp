#include "simulator_wallet_metrics_json.h"

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <utility>

#include "bbp/drivers/chain_wallet_snapshot.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/options.h"
#include "bbp/util.h"

namespace bbp::simulator_app_internal {
namespace {

boost::json::object WalletTransactionJson(
    const ChainWalletTransaction& transaction) {
  boost::json::object object;
  object["direction"] =
      ChainWalletTransactionDirectionName(transaction.direction);
  object["amount_satoshis"] = transaction.amount_satoshis;
  object["confirmations"] = transaction.confirmations;
  object["timestamp"] = transaction.timestamp;
  if (!transaction.txid.empty()) {
    object["txid"] = transaction.txid;
  }
  if (!transaction.address.empty()) {
    object["address"] = transaction.address;
  }
  if (transaction.fee_satoshis) {
    object["fee_satoshis"] = *transaction.fee_satoshis;
  }
  if (!transaction.block_hash.empty()) {
    object["block_hash"] = transaction.block_hash;
  }
  if (transaction.abandoned) {
    object["abandoned"] = *transaction.abandoned;
  }
  return object;
}

}  // namespace

std::string WalletMetricsJson(const Options& options,
                              std::uint32_t wallet_index,
                              std::uint32_t one_based_node,
                              WalletPrivacyMode wallet_mode,
                              const ChainWalletSnapshot& snapshot) {
  boost::json::object object;
  object["run_id"] = options.run_id;
  object["timestamp_ms"] = NowUnixMillis();
  object["wallet_index"] = wallet_index;
  object["node"] = one_based_node;
  object["mode"] = std::string(WalletPrivacyModeName(wallet_mode));
  object["available_balance_satoshis"] = snapshot.available_balance_satoshis;
  object["unconfirmed_balance_satoshis"] =
      snapshot.unconfirmed_balance_satoshis;
  object["immature_balance_satoshis"] = snapshot.immature_balance_satoshis;
  object["transaction_count"] = snapshot.transaction_count;
  object["transaction_history_truncated"] =
      snapshot.transaction_history_truncated;
  boost::json::array transactions;
  transactions.reserve(snapshot.transactions.size());
  for (const ChainWalletTransaction& transaction : snapshot.transactions) {
    transactions.push_back(WalletTransactionJson(transaction));
  }
  object["transactions"] = std::move(transactions);
  return boost::json::serialize(object);
}

}  // namespace bbp::simulator_app_internal
