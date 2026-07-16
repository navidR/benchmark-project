#include "bbp/tui_command_parser.h"

#include <array>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/tokenizer.hpp>
#include <chrono>
#include <stdexcept>
#include <string>
#include <vector>

namespace bbp {
namespace {

constexpr std::array<std::string_view, 24> kCommandNames = {
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
    "network-profile",
    "network-condition",
    "block",
    "unblock",
    "clear-rule",
    "partition",
    "heal",
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

}  // namespace

ParsedTuiCommand TuiCommandParser::Parse(std::string_view input,
                                         std::uint64_t block_production_seed) {
  const std::vector<std::string> tokens = Tokens(input);
  if (tokens.empty()) {
    throw std::runtime_error("command cannot be empty");
  }

  try {
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
          .network_condition = std::nullopt,
          .network_flow = std::nullopt,
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
          .network_condition = std::nullopt,
          .network_flow = std::nullopt,
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
          .network_condition = std::nullopt,
          .network_flow = std::nullopt,
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
          .network_condition = std::nullopt,
          .network_flow = std::nullopt,
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
          .network_condition = std::nullopt,
          .network_flow = std::nullopt,
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
          .network_condition = std::nullopt,
          .network_flow = std::nullopt,
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
          .network_condition = condition,
          .network_flow = std::nullopt,
      };
    }
    if (tokens[0] == "block" || tokens[0] == "unblock") {
      if (tokens.size() != 3U && tokens.size() != 4U) {
        throw std::runtime_error("usage: " + tokens[0] +
                                 " <dst-ipv4> <dst-port> [src-ipv4]");
      }
      const std::uint32_t dst_port =
          boost::lexical_cast<std::uint32_t>(tokens[2]);
      if (dst_port == 0U || dst_port > 65535U) {
        throw std::runtime_error("network flow port must be 1..65535");
      }
      ValidateIpv4Address(tokens[1], "network flow destination");
      if (tokens.size() == 4U) {
        ValidateIpv4Address(tokens[3], "network flow source");
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
          .network_condition = std::nullopt,
          .network_flow =
              SimulationNetworkFlow{
                  .src_address = tokens.size() == 4U ? tokens[3] : "",
                  .dst_address = tokens[1],
                  .dst_port = static_cast<std::uint16_t>(dst_port),
                  .handle = 0U,
              },
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
          .network_condition = std::nullopt,
          .network_flow =
              SimulationNetworkFlow{
                  .src_address = {},
                  .dst_address = {},
                  .dst_port = 0U,
                  .handle = handle,
              },
      };
    }
    if (tokens[0] == "partition" || tokens[0] == "heal") {
      RequireArgumentCount(tokens, 2U, tokens[0] + " <simulation-node-id>");
      return ParsedTuiCommand{
          .kind = tokens[0] == "partition"
                      ? SimulationCommandKind::kPartitionNodes
                      : SimulationCommandKind::kHealPartition,
          .block_production_policy = std::nullopt,
          .mining_difficulty = std::nullopt,
          .peer_node_id = tokens[1],
          .peer_count_policy = std::nullopt,
          .block_count = std::nullopt,
          .profile = std::nullopt,
          .network_condition = std::nullopt,
          .network_flow = std::nullopt,
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
        .network_condition = std::nullopt,
        .network_flow = std::nullopt,
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
