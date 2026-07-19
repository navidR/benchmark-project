#include "bbp/chain_extra_args.h"

#include <algorithm>
#include <array>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace bbp {
namespace {

constexpr std::size_t kMaximumArgumentCount = 32U;
constexpr std::size_t kMaximumArgumentBytes = 1024U;
constexpr std::size_t kMaximumTotalBytes = 8192U;

std::string NormalizedOptionName(std::string_view argument) {
  std::size_t prefix = 0U;
  while (prefix < argument.size() && argument[prefix] == '-') {
    ++prefix;
  }
  if (prefix == 0U || prefix > 2U || prefix == argument.size()) {
    throw std::runtime_error(
        "chain extra argument must be a named daemon option");
  }
  const std::size_t equals = argument.find('=', prefix);
  const std::string_view raw_name = argument.substr(
      prefix, equals == std::string_view::npos ? std::string_view::npos
                                               : equals - prefix);
  if (raw_name.empty()) {
    throw std::runtime_error(
        "chain extra argument must have a nonempty option name");
  }
  std::string name;
  name.reserve(raw_name.size());
  for (const unsigned char character : raw_name) {
    const bool lower = character >= 'a' && character <= 'z';
    const bool upper = character >= 'A' && character <= 'Z';
    const bool digit = character >= '0' && character <= '9';
    if (!(lower || upper || digit || character == '-')) {
      throw std::runtime_error(
          "chain extra argument option name contains an unsafe character");
    }
    name.push_back(upper ? static_cast<char>(character - 'A' + 'a')
                         : static_cast<char>(character));
  }
  return name;
}

bool ContainsAny(std::string_view name,
                 const std::array<std::string_view, 12U>& fragments) {
  return std::any_of(fragments.begin(), fragments.end(),
                     [name](std::string_view fragment) {
                       return name.find(fragment) != std::string_view::npos;
                     });
}

bool IsSimulatorOwnedOptionName(std::string_view name) {
  constexpr std::array<std::string_view, 12U> kOwnedFragments = {
      "auth", "bind",   "connect",  "cookie", "key",  "listen",
      "log",  "notify", "password", "port",   "seed", "wallet",
  };
  constexpr auto kOwnedNames = std::to_array<std::string_view>({
      "add-exclusive-node",
      "addnode",
      "allow-local-ip",
      "banscore",
      "bantime",
      "chain",
      "check-updates",
      "conf",
      "confirm-external-bind",
      "daemon",
      "daemonwait",
      "dandelion",
      "data-dir",
      "datadir",
      "debug",
      "debugexclude",
      "devnet",
      "disable-dns-checkpoints",
      "discover",
      "dns",
      "dropmessagestest",
      "externalip",
      "fallbackfee",
      "fixed-difficulty",
      "fixedseeds",
      "forcednsseed",
      "fuzzmessagestest",
      "gen",
      "genproclimit",
      "help",
      "includeconf",
      "in-peers",
      "keep-fakechain",
      "loadblock",
      "mainnet",
      "max-connections-per-ip",
      "maxconnections",
      "maxreceivebuffer",
      "maxsendbuffer",
      "maxuploadtarget",
      "minrelaytxfee",
      "maxtxfee",
      "mintxfee",
      "mocktime",
      "mnemonic",
      "mnemonicpassphrase",
      "network",
      "no-igd",
      "no-zmq",
      "non-interactive",
      "onlynet",
      "onion",
      "out-peers",
      "pid",
      "paytxfee",
      "printtoconsole",
      "prune",
      "proxy",
      "proxyrandomize",
      "regtest",
      "reindex",
      "reindex-chainstate",
      "rest",
      "rpcallowip",
      "rpcbind",
      "rpcpassword",
      "rpcport",
      "rpcuser",
      "rescan",
      "salvagewallet",
      "server",
      "settings",
      "shrinkdebugfile",
      "stopafterblockimport",
      "sysperms",
      "testnet",
      "timeout",
      "torsetup",
      "torcontrol",
      "torpassword",
      "txindex",
      "txconfirmtarget",
      "upnp",
      "usehd",
      "usemnemonic",
      "version",
      "whitebind",
      "whitelist",
      "whitelistforcerelay",
      "whitelistrelay",
  });
  constexpr auto kOwnedPrefixes = std::to_array<std::string_view>({
      "debug",
      "log",
      "rpc",
      "tor",
      "wallet",
      "zmq",
  });
  const auto is_owned = [&](std::string_view candidate) {
    if (ContainsAny(candidate, kOwnedFragments) ||
        std::find(kOwnedNames.begin(), kOwnedNames.end(), candidate) !=
            kOwnedNames.end()) {
      return true;
    }
    return std::any_of(kOwnedPrefixes.begin(), kOwnedPrefixes.end(),
                       [candidate](std::string_view prefix) {
                         return candidate.starts_with(prefix);
                       });
  };
  if (is_owned(name)) {
    return true;
  }
  return name.starts_with("no") && name.size() > 2U &&
         is_owned(name.substr(2U));
}

void ValidateArgument(std::string_view argument, std::size_t* total_bytes) {
  if (argument.empty()) {
    throw std::runtime_error("chain extra argument must not be empty");
  }
  if (argument.size() > kMaximumArgumentBytes) {
    throw std::runtime_error("chain extra argument exceeds 1024 bytes");
  }
  for (const unsigned char character : argument) {
    if (character == 0U || character < 0x20U || character == 0x7fU) {
      throw std::runtime_error(
          "chain extra argument must not contain control characters");
    }
  }
  if (*total_bytes > kMaximumTotalBytes - argument.size()) {
    throw std::runtime_error("chain extra_args exceeds 8192 total bytes");
  }
  *total_bytes += argument.size();
  const std::string option_name = NormalizedOptionName(argument);
  if (IsSimulatorOwnedOptionName(option_name)) {
    throw std::runtime_error(
        "chain extra argument uses simulator-owned option: " + option_name);
  }
}

}  // namespace

ChainExtraArgs::ChainExtraArgs(std::vector<std::string> arguments) {
  if (arguments.size() > kMaximumArgumentCount) {
    throw std::runtime_error(
        "chain extra_args must contain at most 32 arguments");
  }
  std::size_t total_bytes = 0U;
  for (const std::string& argument : arguments) {
    ValidateArgument(argument, &total_bytes);
  }
  arguments_ = std::move(arguments);
}

const std::vector<std::string>& ChainExtraArgs::arguments() const noexcept {
  return arguments_;
}

bool ChainExtraArgs::empty() const noexcept { return arguments_.empty(); }

}  // namespace bbp
