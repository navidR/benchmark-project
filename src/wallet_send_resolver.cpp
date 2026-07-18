#include "bbp/wallet_send_resolver.h"

#include <boost/json/array.hpp>
#include <boost/json/value.hpp>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>

namespace bbp {
namespace {

const boost::json::array& RequireArray(const boost::json::object& report,
                                       std::string_view field) {
  const boost::json::value* value = report.if_contains(field);
  if (value == nullptr || !value->is_array()) {
    throw std::runtime_error("live report has no valid " + std::string(field));
  }
  return value->as_array();
}

std::uint64_t RequirePositiveUnsigned(const boost::json::object& object,
                                      std::string_view field,
                                      std::string_view context) {
  const boost::json::value* value = object.if_contains(field);
  if (value != nullptr && value->is_uint64() && value->as_uint64() != 0U) {
    return value->as_uint64();
  }
  if (value != nullptr && value->is_int64() && value->as_int64() > 0) {
    return static_cast<std::uint64_t>(value->as_int64());
  }
  throw std::runtime_error(std::string(context) + " has no valid " +
                           std::string(field));
}

const boost::json::object& RequireSelectedWallet(
    const boost::json::array& wallets, std::size_t selected_wallet) {
  if (selected_wallet >= wallets.size() ||
      !wallets[selected_wallet].is_object()) {
    throw std::runtime_error("no valid selected wallet");
  }
  return wallets[selected_wallet].as_object();
}

const boost::json::object& FindWallet(const boost::json::array& wallets,
                                      std::uint32_t wallet_index) {
  const boost::json::object* match = nullptr;
  for (const boost::json::value& value : wallets) {
    if (!value.is_object()) {
      continue;
    }
    const boost::json::object& wallet = value.as_object();
    const boost::json::value* index = wallet.if_contains("wallet_index");
    const bool matches =
        index != nullptr &&
        ((index->is_uint64() && index->as_uint64() == wallet_index) ||
         (index->is_int64() && index->as_int64() > 0 &&
          static_cast<std::uint64_t>(index->as_int64()) == wallet_index));
    if (!matches) {
      continue;
    }
    if (match != nullptr) {
      throw std::runtime_error("duplicate wallet index: " +
                               std::to_string(wallet_index));
    }
    match = &wallet;
  }
  if (match == nullptr) {
    throw std::runtime_error("unknown receiver wallet: " +
                             std::to_string(wallet_index));
  }
  return *match;
}

std::string BackingNodeId(const boost::json::object& report,
                          const boost::json::object& wallet) {
  const std::uint64_t node_index =
      RequirePositiveUnsigned(wallet, "node", "wallet summary");
  const boost::json::array& nodes = RequireArray(report, "nodes_summary");
  const boost::json::object* match = nullptr;
  for (const boost::json::value& value : nodes) {
    if (!value.is_object()) {
      continue;
    }
    const boost::json::object& node = value.as_object();
    const boost::json::value* index = node.if_contains("node_index");
    const bool matches =
        index != nullptr &&
        ((index->is_uint64() && index->as_uint64() == node_index) ||
         (index->is_int64() && index->as_int64() > 0 &&
          static_cast<std::uint64_t>(index->as_int64()) == node_index));
    if (!matches) {
      continue;
    }
    if (match != nullptr) {
      throw std::runtime_error("duplicate wallet backing node index: " +
                               std::to_string(node_index));
    }
    match = &node;
  }
  if (match == nullptr) {
    throw std::runtime_error("unknown wallet backing node index: " +
                             std::to_string(node_index));
  }
  const boost::json::value* node_id = match->if_contains("node_id");
  if (node_id == nullptr || !node_id->is_string() ||
      node_id->as_string().empty()) {
    throw std::runtime_error("wallet backing node has no valid node_id");
  }
  return std::string(node_id->as_string());
}

std::uint32_t CheckedWalletIndex(const boost::json::object& wallet,
                                 std::string_view context) {
  const std::uint64_t index =
      RequirePositiveUnsigned(wallet, "wallet_index", context);
  if (index > std::numeric_limits<std::uint32_t>::max()) {
    throw std::runtime_error(std::string(context) +
                             " wallet_index exceeds uint32");
  }
  return static_cast<std::uint32_t>(index);
}

}  // namespace

ResolvedWalletSend ResolveSelectedWalletSend(const boost::json::object& report,
                                             std::size_t selected_wallet,
                                             SimulationWalletSend send) {
  if (send.sender_wallet_index != 0U) {
    throw std::runtime_error(
        "unresolved wallet send must not specify a sender wallet");
  }
  if (send.receiver_wallet_index == 0U) {
    throw std::runtime_error("wallet send requires a receiver wallet");
  }
  if (send.amount_satoshis == 0U) {
    throw std::runtime_error("wallet send amount must be greater than zero");
  }
  if (send.amount_satoshis >
      std::numeric_limits<std::uint64_t>::max() - send.fee_satoshis) {
    throw std::runtime_error("wallet send amount plus fee overflows uint64");
  }
  if (send.timeout_sec == 0U) {
    throw std::runtime_error("wallet send timeout must be greater than zero");
  }

  const boost::json::array& wallets = RequireArray(report, "wallets_summary");
  const boost::json::object& sender =
      RequireSelectedWallet(wallets, selected_wallet);
  send.sender_wallet_index = CheckedWalletIndex(sender, "selected wallet");
  static_cast<void>(FindWallet(wallets, send.sender_wallet_index));
  const boost::json::object& receiver =
      FindWallet(wallets, send.receiver_wallet_index);
  const std::uint32_t canonical_receiver_index =
      CheckedWalletIndex(receiver, "receiver wallet");
  if (canonical_receiver_index != send.receiver_wallet_index) {
    throw std::runtime_error("receiver wallet index is not canonical");
  }
  if (send.sender_wallet_index == send.receiver_wallet_index) {
    throw std::runtime_error("wallet send source and receiver must differ");
  }

  const std::string sender_node_id = BackingNodeId(report, sender);
  static_cast<void>(BackingNodeId(report, receiver));
  return ResolvedWalletSend{
      .send = send,
      .sender_node_id = sender_node_id,
      .target_text = "wallet-" + std::to_string(send.sender_wallet_index) +
                     " -> wallet-" + std::to_string(send.receiver_wallet_index),
  };
}

}  // namespace bbp
