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

constexpr std::array<std::string_view, 10> kCommandNames = {
    "block-production", "mining-difficulty", "stop-mining",     "disconnect",
    "reconnect",        "connect-peer",      "disconnect-peer", "peer-policy",
    "log-more",         "log-less",
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
    } else {
      throw std::runtime_error("unknown command: " + tokens[0]);
    }
    return ParsedTuiCommand{
        .kind = kind,
        .block_production_policy = std::nullopt,
        .mining_difficulty = std::nullopt,
        .peer_node_id = std::nullopt,
        .peer_count_policy = std::nullopt,
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
