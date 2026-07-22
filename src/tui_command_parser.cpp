#include "bbp/tui_command_parser.h"

#include <array>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/tokenizer.hpp>
#include <charconv>
#include <chrono>
#include <limits>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "bbp/util.h"

namespace bbp {
namespace {

constexpr std::array<std::string_view, 29> kCommandNames = {
    "block-production",
    "mining-difficulty",
    "stop-mining",
    "disconnect",
    "reconnect",
    "connect-peer",
    "disconnect-peer",
    "peer-policy",
    "log-more",
    "log-less",
    "freeze",
    "thaw",
    "stop-node",
    "restart",
    "kill",
    "generate-blocks",
    "resource-profile",
    "resource-limit",
    "network-profile",
    "network-condition",
    "block",
    "unblock",
    "clear-rule",
    "partition",
    "heal",
    "export-node-report",
    "perf-counters",
    "wallet-send",
    "firo-qt",
};

std::vector<std::string> Tokens(std::string_view input) {
  const std::string text = boost::algorithm::trim_copy(std::string(input));
  boost::char_separator<char> separator(" \t");
  boost::tokenizer<boost::char_separator<char>> tokenizer(text, separator);
  return {tokenizer.begin(), tokenizer.end()};
}

void RequireArgumentCount(const std::vector<std::string>& tokens,
                          std::size_t expected, std::string_view usage) {
  if (tokens.size() != expected) {
    throw std::runtime_error("usage: " + std::string(usage));
  }
}

std::vector<PerfCounterKind> ParsePerfCounterKinds(std::string_view text) {
  if (text.empty()) {
    throw std::runtime_error("perf counter selection must not be empty");
  }
  std::vector<PerfCounterKind> kinds;
  std::set<PerfCounterKind> unique;
  std::size_t begin = 0U;
  while (begin <= text.size()) {
    const std::size_t comma = text.find(',', begin);
    const std::size_t end =
        comma == std::string_view::npos ? text.size() : comma;
    const std::string_view name = text.substr(begin, end - begin);
    if (name.empty()) {
      throw std::runtime_error("perf counter selection contains an empty name");
    }
    const std::optional<PerfCounterKind> kind = PerfCounterKindFromName(name);
    if (!kind) {
      throw std::runtime_error("unknown perf counter: " + std::string(name));
    }
    if (!unique.insert(*kind).second) {
      throw std::runtime_error("duplicate perf counter: " + std::string(name));
    }
    kinds.push_back(*kind);
    if (comma == std::string_view::npos) {
      break;
    }
    begin = comma + 1U;
  }
  return kinds;
}

std::uint64_t ParseUnsignedToken(std::string_view text,
                                 std::string_view field) {
  std::uint64_t value = 0U;
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value, 10);
  if (error == std::errc::result_out_of_range) {
    throw std::runtime_error(std::string(field) + " exceeds uint64");
  }
  if (error != std::errc{} || end != text.data() + text.size()) {
    throw std::runtime_error(std::string(field) +
                             " must be an unsigned decimal integer");
  }
  return value;
}

std::uint64_t ParsePositiveToken(std::string_view text,
                                 std::string_view field) {
  const std::uint64_t value = ParseUnsignedToken(text, field);
  if (value == 0U) {
    throw std::runtime_error(std::string(field) + " must be greater than zero");
  }
  return value;
}

std::optional<std::uint64_t> ParsePositiveOrMaxToken(std::string_view text,
                                                     std::string_view field) {
  if (text == "max") {
    return std::nullopt;
  }
  return ParsePositiveToken(text, field);
}

}  // namespace

ParsedTuiCommand TuiCommandParser::Parse(std::string_view input,
                                         std::uint64_t block_production_seed) {
  const std::vector<std::string> tokens = Tokens(input);
  if (tokens.empty()) {
    throw std::runtime_error("command cannot be empty");
  }

  try {
    if (tokens[0] == "firo-qt") {
      RequireArgumentCount(tokens, 1U, "firo-qt");
      ParsedTuiCommand parsed;
      parsed.local_action = TuiLocalAction::kCreateFiroQtLauncher;
      return parsed;
    }
    if (tokens[0] == "resource-limit") {
      ResourceLimitPatch patch;
      if (tokens.size() < 3U) {
        throw std::runtime_error(
            "usage: resource-limit <memory-high|memory-max|cpu-max|"
            "cpu-weight|io-max|io-weight|pids-max> <value...>");
      }
      if (tokens[1] == "memory-high") {
        RequireArgumentCount(tokens, 3U, "resource-limit memory-high <bytes>");
        patch.memory_high_bytes =
            ParseUnsignedToken(tokens[2], "memory-high bytes");
      } else if (tokens[1] == "memory-max") {
        RequireArgumentCount(tokens, 3U, "resource-limit memory-max <bytes>");
        patch.memory_max_bytes =
            ParsePositiveToken(tokens[2], "memory-max bytes");
      } else if (tokens[1] == "cpu-max") {
        RequireArgumentCount(
            tokens, 4U, "resource-limit cpu-max <quota-us|max> <period-us>");
        patch.cpu_quota_present = true;
        patch.cpu_quota_us =
            ParsePositiveOrMaxToken(tokens[2], "cpu-max quota");
        patch.cpu_period_us = ParsePositiveToken(tokens[3], "cpu-max period");
      } else if (tokens[1] == "cpu-weight") {
        RequireArgumentCount(tokens, 3U,
                             "resource-limit cpu-weight <1..10000>");
        patch.cpu_weight = ParseUnsignedToken(tokens[2], "cpu-weight value");
      } else if (tokens[1] == "io-max") {
        RequireArgumentCount(
            tokens, 7U,
            "resource-limit io-max <major:minor> <rbps|max> <wbps|max> "
            "<riops|max> <wiops|max>");
        patch.io_limits_present = true;
        patch.io_limits.push_back(IoLimit{
            .device = ParseBlockDeviceId(tokens[2]),
            .read_bytes_per_sec =
                ParsePositiveOrMaxToken(tokens[3], "io-max rbps"),
            .write_bytes_per_sec =
                ParsePositiveOrMaxToken(tokens[4], "io-max wbps"),
            .read_operations_per_sec =
                ParsePositiveOrMaxToken(tokens[5], "io-max riops"),
            .write_operations_per_sec =
                ParsePositiveOrMaxToken(tokens[6], "io-max wiops"),
        });
      } else if (tokens[1] == "io-weight") {
        RequireArgumentCount(tokens, 3U, "resource-limit io-weight <1..10000>");
        patch.io_weight = ParseUnsignedToken(tokens[2], "io-weight value");
      } else if (tokens[1] == "pids-max") {
        RequireArgumentCount(tokens, 3U,
                             "resource-limit pids-max <positive-count>");
        patch.pids_max = ParsePositiveToken(tokens[2], "pids-max value");
      } else {
        throw std::runtime_error("unknown resource limit: " + tokens[1]);
      }
      ValidateResourceLimitPatch(patch, "operator resource update");
      ParsedTuiCommand parsed;
      parsed.kind = SimulationCommandKind::kSetResourceLimits;
      parsed.resource_limit_patch = std::move(patch);
      return parsed;
    }
    if (tokens[0] == "perf-counters") {
      if (tokens.size() < 2U || tokens.size() > 4U) {
        throw std::runtime_error(
            "usage: perf-counters [node|wallet|group|cgroup [target-id]] "
            "<counter[,counter...]>");
      }
      std::optional<PerfCounterTargetKind> target_kind;
      std::optional<std::string> target_id;
      if (tokens.size() >= 3U) {
        target_kind = PerfCounterTargetKindFromName(tokens[1]);
        if (!target_kind) {
          throw std::runtime_error("unknown perf counter target kind: " +
                                   tokens[1]);
        }
      }
      if (tokens.size() == 4U) {
        if (tokens[2].empty()) {
          throw std::runtime_error("perf counter target id must not be empty");
        }
        target_id = tokens[2];
      }
      return ParsedTuiCommand{
          .kind = SimulationCommandKind::kSetPerfCounters,
          .block_production_policy = std::nullopt,
          .mining_difficulty = std::nullopt,
          .peer_node_id = std::nullopt,
          .peer_count_policy = std::nullopt,
          .block_count = std::nullopt,
          .profile = std::nullopt,
          .resource_limit_patch = std::nullopt,
          .network_condition = std::nullopt,
          .network_flow = std::nullopt,
          .partition_target_kind = std::nullopt,
          .perf_counter_target_kind = target_kind,
          .perf_counter_target_id = std::move(target_id),
          .perf_counter_kinds = ParsePerfCounterKinds(tokens.back()),
          .wallet_send = std::nullopt,
      };
    }
    if (tokens[0] == "wallet-send") {
      if (tokens.size() != 4U && tokens.size() != 5U) {
        throw std::runtime_error(
            "usage: wallet-send <receiver-wallet-index> <amount> <fee> "
            "[timeout-sec]");
      }
      const std::uint64_t receiver =
          ParsePositiveToken(tokens[1], "receiver wallet index");
      if (receiver > std::numeric_limits<std::uint32_t>::max()) {
        throw std::runtime_error("receiver wallet index exceeds uint32");
      }
      const std::uint64_t amount =
          ParseFixed8Amount(tokens[2], "wallet amount");
      const std::uint64_t fee = ParseFixed8Amount(tokens[3], "wallet fee");
      if (amount == 0U) {
        throw std::runtime_error(
            "wallet send amount must be greater than zero");
      }
      if (amount > std::numeric_limits<std::uint64_t>::max() - fee) {
        throw std::runtime_error(
            "wallet send amount plus fee overflows uint64");
      }
      std::uint64_t timeout = 30U;
      if (tokens.size() == 5U) {
        timeout = ParsePositiveToken(tokens[4], "wallet send timeout");
        if (timeout > std::numeric_limits<std::uint32_t>::max()) {
          throw std::runtime_error("wallet send timeout exceeds uint32");
        }
      }
      return ParsedTuiCommand{
          .kind = SimulationCommandKind::kSendWalletTransaction,
          .block_production_policy = std::nullopt,
          .mining_difficulty = std::nullopt,
          .peer_node_id = std::nullopt,
          .peer_count_policy = std::nullopt,
          .block_count = std::nullopt,
          .profile = std::nullopt,
          .resource_limit_patch = std::nullopt,
          .network_condition = std::nullopt,
          .network_flow = std::nullopt,
          .partition_target_kind = std::nullopt,
          .perf_counter_target_kind = std::nullopt,
          .perf_counter_target_id = std::nullopt,
          .perf_counter_kinds = {},
          .wallet_send =
              SimulationWalletSend{
                  .sender_wallet_index = 0U,
                  .receiver_wallet_index = static_cast<std::uint32_t>(receiver),
                  .amount_satoshis = amount,
                  .fee_satoshis = fee,
                  .timeout_sec = static_cast<std::uint32_t>(timeout),
              },
      };
    }
    if (tokens[0] == "block-production") {
      RequireArgumentCount(tokens, 3U,
                           "block-production <probability> <period-ms>");
      const double probability = boost::lexical_cast<double>(tokens[1]);
      const std::uint32_t period_ms =
          boost::lexical_cast<std::uint32_t>(tokens[2]);
      return ParsedTuiCommand{
          .kind = SimulationCommandKind::kSetBlockProductionPolicy,
          .block_production_policy =
              BlockProductionPolicy(std::chrono::milliseconds(period_ms),
                                    probability, block_production_seed),
          .mining_difficulty = std::nullopt,
          .peer_node_id = std::nullopt,
          .peer_count_policy = std::nullopt,
          .block_count = std::nullopt,
          .profile = std::nullopt,
          .resource_limit_patch = std::nullopt,
          .network_condition = std::nullopt,
          .network_flow = std::nullopt,
          .partition_target_kind = std::nullopt,
          .perf_counter_target_kind = std::nullopt,
          .perf_counter_target_id = std::nullopt,
          .perf_counter_kinds = {},
          .wallet_send = std::nullopt,
      };
    }
    if (tokens[0] == "mining-difficulty") {
      RequireArgumentCount(tokens, 2U, "mining-difficulty <positive-value>");
      return ParsedTuiCommand{
          .kind = SimulationCommandKind::kSetMiningDifficulty,
          .block_production_policy = std::nullopt,
          .mining_difficulty =
              MiningDifficulty(boost::lexical_cast<double>(tokens[1])),
          .peer_node_id = std::nullopt,
          .peer_count_policy = std::nullopt,
          .block_count = std::nullopt,
          .profile = std::nullopt,
          .resource_limit_patch = std::nullopt,
          .network_condition = std::nullopt,
          .network_flow = std::nullopt,
          .partition_target_kind = std::nullopt,
          .perf_counter_target_kind = std::nullopt,
          .perf_counter_target_id = std::nullopt,
          .perf_counter_kinds = {},
          .wallet_send = std::nullopt,
      };
    }
    if (tokens[0] == "connect-peer" || tokens[0] == "disconnect-peer") {
      RequireArgumentCount(tokens, 2U, tokens[0] + " <simulation-node-id>");
      return ParsedTuiCommand{
          .kind = tokens[0] == "connect-peer"
                      ? SimulationCommandKind::kConnectPeer
                      : SimulationCommandKind::kDisconnectPeer,
          .block_production_policy = std::nullopt,
          .mining_difficulty = std::nullopt,
          .peer_node_id = tokens[1],
          .peer_count_policy = std::nullopt,
          .block_count = std::nullopt,
          .profile = std::nullopt,
          .resource_limit_patch = std::nullopt,
          .network_condition = std::nullopt,
          .network_flow = std::nullopt,
          .partition_target_kind = std::nullopt,
          .perf_counter_target_kind = std::nullopt,
          .perf_counter_target_id = std::nullopt,
          .perf_counter_kinds = {},
          .wallet_send = std::nullopt,
      };
    }
    if (tokens[0] == "peer-policy") {
      RequireArgumentCount(tokens, 3U, "peer-policy <minimum> <maximum>");
      return ParsedTuiCommand{
          .kind = SimulationCommandKind::kSetPeerCountPolicy,
          .block_production_policy = std::nullopt,
          .mining_difficulty = std::nullopt,
          .peer_node_id = std::nullopt,
          .peer_count_policy =
              PeerCountPolicy(boost::lexical_cast<std::uint32_t>(tokens[1]),
                              boost::lexical_cast<std::uint32_t>(tokens[2])),
          .block_count = std::nullopt,
          .profile = std::nullopt,
          .resource_limit_patch = std::nullopt,
          .network_condition = std::nullopt,
          .network_flow = std::nullopt,
          .partition_target_kind = std::nullopt,
          .perf_counter_target_kind = std::nullopt,
          .perf_counter_target_id = std::nullopt,
          .perf_counter_kinds = {},
          .wallet_send = std::nullopt,
      };
    }
    if (tokens[0] == "generate-blocks") {
      RequireArgumentCount(tokens, 2U, "generate-blocks <positive-count>");
      const std::uint32_t block_count =
          boost::lexical_cast<std::uint32_t>(tokens[1]);
      if (block_count == 0U) {
        throw std::runtime_error("generate-blocks count must be positive");
      }
      return ParsedTuiCommand{
          .kind = SimulationCommandKind::kGenerateBlocks,
          .block_production_policy = std::nullopt,
          .mining_difficulty = std::nullopt,
          .peer_node_id = std::nullopt,
          .peer_count_policy = std::nullopt,
          .block_count = block_count,
          .profile = std::nullopt,
          .resource_limit_patch = std::nullopt,
          .network_condition = std::nullopt,
          .network_flow = std::nullopt,
          .partition_target_kind = std::nullopt,
          .perf_counter_target_kind = std::nullopt,
          .perf_counter_target_id = std::nullopt,
          .perf_counter_kinds = {},
          .wallet_send = std::nullopt,
      };
    }
    if (tokens[0] == "resource-profile" || tokens[0] == "network-profile") {
      RequireArgumentCount(tokens, 2U, tokens[0] + " <name>");
      return ParsedTuiCommand{
          .kind = tokens[0] == "resource-profile"
                      ? SimulationCommandKind::kSetResourceProfile
                      : SimulationCommandKind::kSetNetworkProfile,
          .block_production_policy = std::nullopt,
          .mining_difficulty = std::nullopt,
          .peer_node_id = std::nullopt,
          .peer_count_policy = std::nullopt,
          .block_count = std::nullopt,
          .profile = tokens[1],
          .resource_limit_patch = std::nullopt,
          .network_condition = std::nullopt,
          .network_flow = std::nullopt,
          .partition_target_kind = std::nullopt,
          .perf_counter_target_kind = std::nullopt,
          .perf_counter_target_id = std::nullopt,
          .perf_counter_kinds = {},
          .wallet_send = std::nullopt,
      };
    }
    if (tokens[0] == "network-condition") {
      if (tokens.size() != 8U && tokens.size() != 9U) {
        throw std::runtime_error(
            "usage: network-condition <bandwidth-mbps> <delay-ms> "
            "<jitter-ms> <loss-bps> <duplicate-bps> <corrupt-bps> "
            "<reorder-bps> [limit-packets]");
      }
      NetworkCondition condition;
      condition.bandwidth_mbps = boost::lexical_cast<std::uint32_t>(tokens[1]);
      condition.delay_ms = boost::lexical_cast<std::uint32_t>(tokens[2]);
      condition.jitter_ms = boost::lexical_cast<std::uint32_t>(tokens[3]);
      condition.loss_basis_points =
          boost::lexical_cast<std::uint32_t>(tokens[4]);
      condition.duplicate_basis_points =
          boost::lexical_cast<std::uint32_t>(tokens[5]);
      condition.corrupt_basis_points =
          boost::lexical_cast<std::uint32_t>(tokens[6]);
      condition.reorder_basis_points =
          boost::lexical_cast<std::uint32_t>(tokens[7]);
      if (tokens.size() == 9U) {
        condition.limit_packets = boost::lexical_cast<std::uint32_t>(tokens[8]);
      }
      ValidateNetworkCondition(condition);
      return ParsedTuiCommand{
          .kind = SimulationCommandKind::kSetNetworkCondition,
          .block_production_policy = std::nullopt,
          .mining_difficulty = std::nullopt,
          .peer_node_id = std::nullopt,
          .peer_count_policy = std::nullopt,
          .block_count = std::nullopt,
          .profile = std::nullopt,
          .resource_limit_patch = std::nullopt,
          .network_condition = condition,
          .network_flow = std::nullopt,
          .partition_target_kind = std::nullopt,
          .perf_counter_target_kind = std::nullopt,
          .perf_counter_target_id = std::nullopt,
          .perf_counter_kinds = {},
          .wallet_send = std::nullopt,
      };
    }
    if (tokens[0] == "block" || tokens[0] == "unblock") {
      if (tokens.size() < 3U || tokens.size() > 5U) {
        throw std::runtime_error("usage: " + tokens[0] +
                                 " <dst-ipv4> <dst-port> [src-ipv4|*] "
                                 "[src-port]");
      }
      const std::uint32_t dst_port =
          boost::lexical_cast<std::uint32_t>(tokens[2]);
      if (dst_port == 0U || dst_port > 65535U) {
        throw std::runtime_error("network flow port must be 1..65535");
      }
      ValidateIpv4Address(tokens[1], "network flow destination");
      if (tokens.size() >= 4U && tokens[3] != "*") {
        ValidateIpv4Address(tokens[3], "network flow source");
      }
      std::uint32_t src_port = 0U;
      if (tokens.size() == 5U) {
        src_port = boost::lexical_cast<std::uint32_t>(tokens[4]);
        if (src_port == 0U || src_port > 65535U) {
          throw std::runtime_error("network flow source port must be 1..65535");
        }
      }
      return ParsedTuiCommand{
          .kind = tokens[0] == "block"
                      ? SimulationCommandKind::kBlockNetworkFlow
                      : SimulationCommandKind::kUnblockNetworkFlow,
          .block_production_policy = std::nullopt,
          .mining_difficulty = std::nullopt,
          .peer_node_id = std::nullopt,
          .peer_count_policy = std::nullopt,
          .block_count = std::nullopt,
          .profile = std::nullopt,
          .resource_limit_patch = std::nullopt,
          .network_condition = std::nullopt,
          .network_flow =
              SimulationNetworkFlow{
                  .src_address =
                      tokens.size() >= 4U && tokens[3] != "*" ? tokens[3] : "",
                  .src_port = static_cast<std::uint16_t>(src_port),
                  .dst_address = tokens[1],
                  .dst_port = static_cast<std::uint16_t>(dst_port),
                  .handle = 0U,
              },
          .partition_target_kind = std::nullopt,
          .perf_counter_target_kind = std::nullopt,
          .perf_counter_target_id = std::nullopt,
          .perf_counter_kinds = {},
          .wallet_send = std::nullopt,
      };
    }
    if (tokens[0] == "clear-rule") {
      RequireArgumentCount(tokens, 2U, "clear-rule <handle>");
      const std::uint32_t handle =
          boost::lexical_cast<std::uint32_t>(tokens[1]);
      if (handle == 0U) {
        throw std::runtime_error("network rule handle must be positive");
      }
      return ParsedTuiCommand{
          .kind = SimulationCommandKind::kUnblockNetworkFlow,
          .block_production_policy = std::nullopt,
          .mining_difficulty = std::nullopt,
          .peer_node_id = std::nullopt,
          .peer_count_policy = std::nullopt,
          .block_count = std::nullopt,
          .profile = std::nullopt,
          .resource_limit_patch = std::nullopt,
          .network_condition = std::nullopt,
          .network_flow =
              SimulationNetworkFlow{
                  .src_address = {},
                  .src_port = 0U,
                  .dst_address = {},
                  .dst_port = 0U,
                  .handle = handle,
              },
          .partition_target_kind = std::nullopt,
          .perf_counter_target_kind = std::nullopt,
          .perf_counter_target_id = std::nullopt,
          .perf_counter_kinds = {},
          .wallet_send = std::nullopt,
      };
    }
    if (tokens[0] == "partition" || tokens[0] == "heal") {
      if (tokens.size() > 2U) {
        throw std::runtime_error("usage: " + tokens[0] +
                                 " [simulation-node-id]");
      }
      const bool selected_group = tokens.size() == 1U;
      return ParsedTuiCommand{
          .kind = tokens[0] == "partition"
                      ? SimulationCommandKind::kPartitionNodes
                      : SimulationCommandKind::kHealPartition,
          .block_production_policy = std::nullopt,
          .mining_difficulty = std::nullopt,
          .peer_node_id = selected_group
                              ? std::nullopt
                              : std::optional<std::string>(tokens[1]),
          .peer_count_policy = std::nullopt,
          .block_count = std::nullopt,
          .profile = std::nullopt,
          .resource_limit_patch = std::nullopt,
          .network_condition = std::nullopt,
          .network_flow = std::nullopt,
          .partition_target_kind =
              selected_group ? TuiPartitionTargetKind::kSelectedTopologyGroup
                             : TuiPartitionTargetKind::kNodePair,
          .perf_counter_target_kind = std::nullopt,
          .perf_counter_target_id = std::nullopt,
          .perf_counter_kinds = {},
          .wallet_send = std::nullopt,
      };
    }

    RequireArgumentCount(tokens, 1U, tokens[0]);
    SimulationCommandKind kind = SimulationCommandKind::kStopMining;
    if (tokens[0] == "stop-mining") {
      kind = SimulationCommandKind::kStopMining;
    } else if (tokens[0] == "disconnect") {
      kind = SimulationCommandKind::kDisconnectNode;
    } else if (tokens[0] == "reconnect") {
      kind = SimulationCommandKind::kReconnectNode;
    } else if (tokens[0] == "log-more") {
      kind = SimulationCommandKind::kIncreaseLogVerbosity;
    } else if (tokens[0] == "log-less") {
      kind = SimulationCommandKind::kDecreaseLogVerbosity;
    } else if (tokens[0] == "freeze") {
      kind = SimulationCommandKind::kFreezeNode;
    } else if (tokens[0] == "thaw") {
      kind = SimulationCommandKind::kThawNode;
    } else if (tokens[0] == "stop-node") {
      kind = SimulationCommandKind::kStopNode;
    } else if (tokens[0] == "restart") {
      kind = SimulationCommandKind::kRestartNode;
    } else if (tokens[0] == "kill") {
      kind = SimulationCommandKind::kKillNode;
    } else if (tokens[0] == "export-node-report") {
      kind = SimulationCommandKind::kExportNodeReport;
    } else {
      throw std::runtime_error("unknown command: " + tokens[0]);
    }
    return ParsedTuiCommand{
        .kind = kind,
        .block_production_policy = std::nullopt,
        .mining_difficulty = std::nullopt,
        .peer_node_id = std::nullopt,
        .peer_count_policy = std::nullopt,
        .block_count = std::nullopt,
        .profile = std::nullopt,
        .resource_limit_patch = std::nullopt,
        .network_condition = std::nullopt,
        .network_flow = std::nullopt,
        .partition_target_kind = std::nullopt,
        .perf_counter_target_kind = std::nullopt,
        .perf_counter_target_id = std::nullopt,
        .perf_counter_kinds = {},
        .wallet_send = std::nullopt,
    };
  } catch (const boost::bad_lexical_cast&) {
    throw std::runtime_error("command contains an invalid numeric argument");
  }
}

std::string TuiCommandParser::Complete(std::string_view input) {
  const std::string prefix = boost::algorithm::trim_copy(std::string(input));
  if (prefix.empty() || prefix.find_first_of(" \t") != std::string::npos) {
    return std::string(input);
  }

  std::string match;
  for (const std::string_view command : kCommandNames) {
    if (!boost::algorithm::starts_with(command, prefix)) {
      continue;
    }
    if (!match.empty()) {
      return std::string(input);
    }
    match = command;
  }
  return match.empty() ? std::string(input) : match + " ";
}

std::span<const std::string_view> TuiCommandParser::CommandNames() {
  return kCommandNames;
}

}  // namespace bbp
